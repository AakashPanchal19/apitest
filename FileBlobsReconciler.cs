using BOBDrive.Models;
using Serilog;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using IOFile = System.IO.File;
using FileModel = BOBDrive.Models.File;

namespace BOBDrive.Services.FileOps
{
    public static class FileBlobsReconciler
    {
        private static readonly ILogger _log = Log.ForContext(typeof(FileBlobsReconciler));

        // Reconcile all blobs: create missing rows, fix refcounts, optionally remove zero-ref blobs
        public static async Task ReconcileAllAsync(bool deleteZeroRefBlobs = false)
        {
            using (var ctx = new CloudStorageDbContext())
            {
                // 1) Create missing FileBlobs for existing BlobHash references
                var referencedHashes = await ctx.Files
                    .AsNoTracking()
                    .Where(f => f.BlobHash != null)
                    .Select(f => f.BlobHash)
                    .Distinct()
                    .ToListAsync();

                foreach (var hash in referencedHashes)
                {
                    if (string.IsNullOrWhiteSpace(hash)) continue;

                    var exists = await ctx.FileBlobs.AnyAsync(b => b.FileHash == hash);
                    if (!exists)
                    {
                        var sample = await ctx.Files.AsNoTracking()
                            .Where(f => f.BlobHash == hash)
                            .OrderByDescending(f => f.Id)
                            .FirstOrDefaultAsync();

                        var blobPath = sample?.FilePath ?? "";
                        long size = 0;
                        try { if (!string.IsNullOrWhiteSpace(blobPath) && IOFile.Exists(blobPath)) size = new System.IO.FileInfo(blobPath).Length; } catch { }

                        ctx.FileBlobs.Add(new FileBlob
                        {
                            FileHash = hash,
                            BlobPath = blobPath,
                            Size = size,
                            CreatedAt = DateTime.UtcNow,
                            RefCount = 0,  // will fix below
                            Pooled = true,
                            LastRefUpdatedAt = DateTime.UtcNow
                        });
                        await ctx.SaveChangesAsync();
                    }
                }

                // 2) Recompute refcount for each blob
                var blobHashes = await ctx.FileBlobs.AsNoTracking().Select(b => b.FileHash).ToListAsync();
                foreach (var hash in blobHashes)
                {
                    await BlobRefHelper.UpdateBlobRefCountFromFilesAsync(ctx, hash);
                }

                // 3) Optionally remove zero-ref blobs
                if (deleteZeroRefBlobs)
                {
                    var zeroRefs = await ctx.FileBlobs.Where(b => b.RefCount == 0).ToListAsync();
                    foreach (var b in zeroRefs)
                    {
                        try
                        {
                            if (!string.IsNullOrWhiteSpace(b.BlobPath) && IOFile.Exists(b.BlobPath))
                                IOFile.Delete(b.BlobPath);
                        }
                        catch (Exception ex)
                        {
                            _log.Warning(ex, "Failed deleting physical blob for hash {Hash}", b.FileHash);
                        }

                        ctx.FileBlobs.Remove(b);
                        await ctx.SaveChangesAsync();
                    }
                }
            }
        }

        // Quick single-hash reconcile (useful after manual fixes)
        public static async Task ReconcileOneAsync(string fileHash)
        {
            if (string.IsNullOrWhiteSpace(fileHash)) return;
            using (var ctx = new CloudStorageDbContext())
            {
                await BlobRefHelper.UpdateBlobRefCountFromFilesAsync(ctx, fileHash);
                var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
                if (blob == null)
                {
                    _log.Information("No FileBlob row exists for hash {Hash}; nothing to reconcile.", fileHash);
                }
                else
                {
                    _log.Information("Reconciled hash {Hash} new RefCount={RefCount}", fileHash, blob.RefCount);
                }
            }
        }
    }
}