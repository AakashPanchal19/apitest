using BOBDrive.Models;
using Serilog;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using IOFile = System.IO.File;      // FS alias
using FileModel = BOBDrive.Models.File;  // Entity alias

namespace BOBDrive.Services.FileOps
{
    internal static class BlobRefHelper
    {
        // Recompute RefCount for a single hash from Files table (BlobHash matches FileBlobs.FileHash)
        public static async Task UpdateBlobRefCountFromFilesAsync(CloudStorageDbContext ctx, string fileHash)
        {
            if (string.IsNullOrWhiteSpace(fileHash)) return;

            // Count file rows referencing this blob
            var count = await ctx.Files.AsNoTracking()
                                       .CountAsync(f => f.BlobHash == fileHash);

            var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
            if (blob != null)
            {
                blob.RefCount = count;
                blob.LastRefUpdatedAt = DateTime.UtcNow;
                await ctx.SaveChangesAsync();
            }
        }

        // Decrement refcount; delete physical blob + row when last reference removed
        public static async Task DecrementBlobRefCountAsync(CloudStorageDbContext ctx, string fileHash, int delta, ILogger log)
        {
            if (string.IsNullOrWhiteSpace(fileHash)) return;

            var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
            if (blob == null)
            {
                // Try to create/fix first
                await UpdateBlobRefCountFromFilesAsync(ctx, fileHash);
                blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
                if (blob == null) return;
            }

            blob.RefCount = Math.Max(0, blob.RefCount - Math.Abs(delta));
            blob.LastRefUpdatedAt = DateTime.UtcNow;
            await ctx.SaveChangesAsync();

            if (blob.RefCount == 0)
            {
                TryDeletePhysical(blob.BlobPath, log);
                ctx.FileBlobs.Remove(blob);
                await ctx.SaveChangesAsync();
            }
        }

        private static void TryDeletePhysical(string path, ILogger log)
        {
            if (string.IsNullOrWhiteSpace(path)) return;
            try
            {
                if (IOFile.Exists(path)) IOFile.Delete(path);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed deleting blob physical {Path}", path);
            }
        }
    }
}