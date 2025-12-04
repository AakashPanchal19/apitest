using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Hosting;
using IOFile = System.IO.File;
using IODirectory = System.IO.Directory;
using FileModel = BOBDrive.Models.File;
using File = BOBDrive.Models.File;

namespace BOBDrive.Services.FileOps
{
    /// <summary>
    /// DeleteService (PathKey-optimized, hierarchy-preserving, fixed physical folder cleanup)
    /// </summary>
    public class DeleteService
    {
        private readonly ILogger _log = Log.ForContext<DeleteService>();

        public Task<string> StartAsync(string externalUserId, string requesterIp, int[] fileIds, int[] folderIds)
        {
            var opId = Guid.NewGuid().ToString("N");
            BOBDrive.Controllers.FileController.BackgroundEnqueue(() =>
                DeleteJobs.Process(opId, externalUserId, requesterIp, fileIds ?? Array.Empty<int>(), folderIds ?? Array.Empty<int>()));
            return Task.FromResult(opId);
        }

        public Task<string> StartEmptyBinAsync(string externalUserId, string requesterIp)
        {
            var opId = Guid.NewGuid().ToString("N");
            BOBDrive.Controllers.FileController.BackgroundEnqueue(() =>
                DeleteJobs.EmptyBin(opId, externalUserId, requesterIp));
            return Task.FromResult(opId);
        }

        // ======================= MAIN DISPATCH =======================

        public static async Task ProcessAsync(string opId, string externalUserId, string requesterIp, int[] fileIds, int[] folderIds)
        {
            var log = Log.ForContext("OpId", opId)
                         .ForContext("UserId", externalUserId)
                         .ForContext("ClientIP", requesterIp);

            log.Information("Delete request: Files={Files}, Folders={Folders}", fileIds?.Length ?? 0, folderIds?.Length ?? 0);

            try
            {
                using (var ctx = new CloudStorageDbContext())
                {
                    var user = await ctx.Users.AsNoTracking().FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    if (user == null) { log.Warning("User not found."); return; }

                    var binFolderId = await EnsureUserBinFolderAsync(ctx, user.Id, user.ExternalUserId);
                    var binDesc = await GetDescendantFoldersAsync(ctx, binFolderId);
                    var binSet = new HashSet<int>(binDesc.Select(f => f.Id)) { binFolderId };

                    fileIds = (fileIds ?? Array.Empty<int>()).Distinct().ToArray();
                    folderIds = (folderIds ?? Array.Empty<int>()).Distinct().ToArray();

                    var userRootPhysical = Path.Combine(UploadConfiguration.FinalUploadPath, user.ExternalUserId);
                    LongPathEnsureDir(userRootPhysical);

                    var binFilesRoot = GetUserBinFilesRoot(user.ExternalUserId);
                    LongPathEnsureDir(binFilesRoot);

                    // Folders
                    foreach (var folderId in folderIds)
                    {
                        var folder = await ctx.Folders.FirstOrDefaultAsync(f => f.Id == folderId);
                        if (folder == null) continue;
                        bool isInBin = binSet.Contains(folder.Id);

                        if (!isInBin)
                        {
                            // Soft delete folder (preserve hierarchy)
                            if (folder.ParentFolderId == null)
                            {
                                log.Information("Skipping root folder Id={Id}", folderId);
                                continue;
                            }

                            var rootPathKey = folder.PathKey;
                            var subtreeIds = !string.IsNullOrWhiteSpace(rootPathKey)
                                ? await GetSubtreeFolderIdsByPathKeyAsync(ctx, rootPathKey)
                                : await GetSubtreeFolderIdsBfsAsync(ctx, folder.Id);

                            folder.ParentFolderId = binFolderId;
                            folder.IsSoftDeleted = true;
                            folder.SoftDeletedAt = DateTime.UtcNow;
                            folder.BinEnteredAt = folder.BinEnteredAt ?? DateTime.UtcNow;
                            await ctx.SaveChangesAsync();

                            var files = await ctx.Files.Where(f => subtreeIds.Contains(f.FolderId)).ToListAsync();
                            foreach (var fm in files)
                            {
                                await SoftDeleteFileInternalAsync(ctx, fm, binFolderId, binFilesRoot, externalUserId, requesterIp, opId, log, preserveHierarchy: true);
                            }

                            log.Information("Soft-deleted folder Id={FolderId} (hierarchy preserved) Folders={FolderCount} Files={FileCount}",
                                folderId, subtreeIds.Count, files.Count);
                        }
                        else
                        {
                            await MarkFolderTreePendingDeleteAsync(ctx, folderId, log);
                            await ctx.SaveChangesAsync();
                            await HardDeleteFolderSubtreeAsync(ctx, folderId, externalUserId, requesterIp, opId, log, userRootPhysical);
                        }
                    }

                    // Files
                    if (fileIds.Length > 0)
                    {
                        var list = await ctx.Files.Where(f => fileIds.Contains(f.Id)).ToListAsync();
                        foreach (var fm in list)
                        {
                            bool inBin = binSet.Contains(fm.FolderId);
                            if (!inBin)
                            {
                                await SoftDeleteFileInternalAsync(ctx, fm, binFolderId, binFilesRoot, externalUserId, requesterIp, opId, log, preserveHierarchy: false);
                            }
                            else
                            {
                                fm.PendingDelete = true;
                                fm.FinalizationStage = "Deleting";
                                await ctx.SaveChangesAsync();
                                await HardDeleteSingleBinFileAsync(ctx, fm, externalUserId, requesterIp, opId, log);
                            }
                        }
                    }

                    log.Information("Delete completed: FilesProcessed={Files} FoldersProcessed={Folders}", fileIds.Length, folderIds.Length);
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "Delete operation failed.");
            }
        }



        /// <summary>
        /// DIRECT HARD DELETE by file/folder IDs, without any user bin logic.
        /// Intended for department bin and admin maintenance, not end-user deletes.
        /// </summary>
        public static async Task HardDeleteDirectAsync(
            string opId,
            string requestedBy,
            string requesterIp,
            int[] fileIds,
            int[] folderIds)
        {
            var log = Log.ForContext("OpId", opId)
                         .ForContext("RequestedBy", requestedBy ?? "(system)")
                         .ForContext("ClientIP", requesterIp);

            fileIds = (fileIds ?? Array.Empty<int>()).Distinct().ToArray();
            folderIds = (folderIds ?? Array.Empty<int>()).Distinct().ToArray();

            log.Information("HardDeleteDirect: Files={Files}, Folders={Folders}", fileIds.Length, folderIds.Length);

            try
            {
                using (var ctx = new CloudStorageDbContext())
                {
                    // Folders first: delete subtrees
                    foreach (var folderId in folderIds)
                    {
                        try
                        {
                            // We don't have a user-root physical path here; we just clean DB and blobs.
                            // Pass empty userRootPhysical; GetPhysicalPathForFolderAsync will still build something,
                            // but SafeDirectoryDeletion will be best-effort.
                            await HardDeleteFolderSubtreeAsync(
                                ctx,
                                folderId,
                                requestedBy ?? "(system)",
                                requesterIp ?? "(dept-bin)",
                                opId,
                                log,
                                userRootPhysical: UploadConfiguration.FinalUploadPath);
                        }
                        catch (Exception ex)
                        {
                            log.Warning(ex, "HardDeleteDirect: folder subtree delete failed FolderId={FolderId}", folderId);
                        }
                    }

                    // Individual files
                    if (fileIds.Length > 0)
                    {
                        var files = await ctx.Files.Where(f => fileIds.Contains(f.Id)).ToListAsync();
                        foreach (var row in files)
                        {
                            try
                            {
                                row.PendingDelete = true;
                                row.FinalizationStage = "Deleting";
                                await ctx.SaveChangesAsync();

                                await HardDeleteSingleBinFileAsync(
                                    ctx,
                                    row,
                                    requestedBy ?? "(system)",
                                    requesterIp ?? "(dept-bin)",
                                    opId,
                                    log);
                            }
                            catch (Exception ex)
                            {
                                log.Warning(ex, "HardDeleteDirect: file delete failed FileId={FileId}", row.Id);
                            }
                        }
                    }

                    log.Information("HardDeleteDirect completed: FilesProcessed={Files} FoldersProcessed={Folders}",
                        fileIds.Length, folderIds.Length);
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "HardDeleteDirect failed.");
            }
        }


        public static async Task EmptyBinAsync(string opId, string externalUserId, string requesterIp)
        {
            var log = Log.ForContext("OpId", opId).ForContext("UserId", externalUserId).ForContext("ClientIP", requesterIp);
            log.Information("EmptyBin started.");

            try
            {
                using (var ctx = new CloudStorageDbContext())
                {
                    var user = await ctx.Users.AsNoTracking().FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    if (user == null) { log.Warning("User not found."); return; }

                    var binFolderId = await EnsureUserBinFolderAsync(ctx, user.Id, user.ExternalUserId);
                    var binPathKey = await ctx.Folders.AsNoTracking().Where(f => f.Id == binFolderId).Select(f => f.PathKey).FirstOrDefaultAsync();
                    var subtreeIds = !string.IsNullOrWhiteSpace(binPathKey)
                        ? await GetSubtreeFolderIdsByPathKeyAsync(ctx, binPathKey)
                        : await GetSubtreeFolderIdsBfsAsync(ctx, binFolderId);

                    var files = await ctx.Files.Where(f => subtreeIds.Contains(f.FolderId)).ToListAsync();
                    foreach (var f in files)
                    {
                        try
                        {
                            f.PendingDelete = true;
                            f.FinalizationStage = "Deleting";
                            await ctx.SaveChangesAsync();
                            await HardDeleteSingleBinFileAsync(ctx, f, externalUserId, requesterIp, opId, log);
                        }
                        catch (Exception ex) { log.Error(ex, "EmptyBin: file delete failed Id={Id}", f.Id); }
                    }

                    foreach (var fid in subtreeIds.OrderByDescending(i => i))
                    {
                        if (fid == binFolderId) continue;
                        var folder = await ctx.Folders.FirstOrDefaultAsync(f => f.Id == fid);
                        if (folder != null) ctx.Folders.Remove(folder);
                    }
                    await ctx.SaveChangesAsync();
                }

                log.Information("EmptyBin completed.");
            }
            catch (Exception ex)
            {
                log.Error(ex, "EmptyBin failed.");
            }
        }

        public static async Task PurgeExpiredBinEntries(ILogger log)
        {
            using (var ctx = new CloudStorageDbContext())
            {
                var cutoff = DateTime.UtcNow.AddDays(-90);

                var expired = await ctx.Files
                    .Where(f => f.IsSoftDeleted &&
                                !f.PendingDelete &&
                                f.BinEnteredAt <= cutoff)
                    .ToListAsync();

                if (!expired.Any())
                {
                    log.Information("No expired bin entries found for purge.");
                    return;
                }

                log.Information("Found {Count} expired bin entries to purge.", expired.Count);

                foreach (var row in expired)
                {
                    try
                    {
                        row.PendingDelete = true;
                        row.FinalizationStage = "Deleting";
                        await ctx.SaveChangesAsync();

                        await HardDeleteSingleBinFileAsync(
                            ctx,
                            row,
                            userId: "(system)",
                            ip: "(purge)",
                            opId: "purge-" + row.Id,
                            baseLog: log);
                    }
                    catch (Exception exOne)
                    {
                        log.Error(exOne, "Failed to hard-delete expired bin file Id={FileId}.", row.Id);
                        // continue with others
                    }
                }
            }
        }

        // ======================= SOFT DELETE FILE =======================

        private static async Task SoftDeleteFileInternalAsync(
            CloudStorageDbContext ctx,
            FileModel fm,
            int binFolderId,
            string binFilesRoot,
            string userId,
            string ip,
            string opId,
            ILogger baseLog,
            bool preserveHierarchy)
        {
            var log = baseLog.ForContext("Action", "SoftDeleteFile").ForContext("FileId", fm.Id);

            var hasHash = !string.IsNullOrWhiteSpace(fm.FileHash);
            var multiRef = hasHash && await ctx.Files.AsNoTracking().AnyAsync(r => r.FileHash == fm.FileHash && r.Id != fm.Id);

            if (multiRef && string.IsNullOrWhiteSpace(fm.BlobHash))
            {
                await EnsurePooledAndRepointAllAsync(ctx, fm, userId, ip, opId, log);
            }
            else if (!multiRef && !preserveHierarchy && !IsSharedPoolPath(fm.FilePath) && !IsInBinFilesPath(fm.FilePath))
            {
                var dest = EnsureUniquePhysical(Path.Combine(binFilesRoot, fm.Name));
                fm.OriginalPhysicalPath = fm.OriginalPhysicalPath ?? fm.FilePath;
                MovePhysicalFileSafe(fm.FilePath, dest, log);
                fm.FilePath = dest;
                log.Information("Relocated unique file to _bin_files Dest={Dest}", dest);
            }

            if (!preserveHierarchy)
            {
                fm.FolderId = binFolderId;
            }

            if (!fm.IsSoftDeleted)
            {
                fm.IsSoftDeleted = true;
                fm.SoftDeletedAt = DateTime.UtcNow;
                fm.BinEnteredAt = DateTime.UtcNow;
            }
            fm.FinalizationStage = "SoftDeleted";
            await ctx.SaveChangesAsync();
            log.Information("Soft delete complete Name={Name}", fm.Name);
        }

        // ======================= HARD DELETE FILE =======================

        // using FileModel = BOBDrive.Models.File; // or your existing alias

        // At top of file, you may already have:
        // using FileModel = BOBDrive.Models.File;

        private static async Task HardDeleteSingleBinFileAsync(
    CloudStorageDbContext ctx,
    File row,
    string userId,
    string ip,
    string opId,
    ILogger baseLog)
        {
            var log = baseLog
                .ForContext("Action", "HardDeleteFile")
                .ForContext("FileId", row.Id)
                .ForContext("UserId", userId)
                .ForContext("IP", ip)
                .ForContext("OpId", opId);

            var hash = row.FileHash ?? "";
            var pooledHash = row.BlobHash;           // if you use pooled blobs
            var physical = ResolvePhysicalPathSafe(row.FilePath);

            // 1) Existing pooling / dedupe logic (leave unchanged)
            if (!string.IsNullOrWhiteSpace(hash) && string.IsNullOrWhiteSpace(pooledHash))
            {
                bool hasOthers = await ctx.Files
                    .AsNoTracking()
                    .AnyAsync(f => f.FileHash == hash && f.Id != row.Id);

                if (hasOthers)
                {
                    await EnsurePooledAndRepointAllAsync(ctx, row, userId, ip, opId, log);

                    // Refresh the entity to get updated BlobHash/FilePath if needed
                    var refreshed = await ctx.Files.FirstOrDefaultAsync(f => f.Id == row.Id);
                    pooledHash = refreshed?.BlobHash;
                    physical = ResolvePhysicalPathSafe(refreshed?.FilePath);
                }
            }

            // 2) NEW: detach this file from any shareable links that reference it
            //    We do NOT delete the ShareableLink itself.
            try
            {
                var linksWithThisFile = await ctx.ShareableLinks
                    .Include(sl => sl.Files)
                    .Where(sl => sl.Files.Any(f => f.Id == row.Id))
                    .ToListAsync();

                foreach (var link in linksWithThisFile)
                {
                    var toRemove = link.Files.FirstOrDefault(f => f.Id == row.Id);
                    if (toRemove != null)
                    {
                        link.Files.Remove(toRemove);
                    }
                }

                if (linksWithThisFile.Count > 0)
                {
                    await ctx.SaveChangesAsync();
                    log.Information(
                        "Detached FileId={FileId} from {Count} shareable links.",
                        row.Id,
                        linksWithThisFile.Count);
                }
            }
            catch (Exception exDetach)
            {
                // Don't block delete if this fails; log and continue.
                log.Error(exDetach, "Failed to detach FileId={FileId} from ShareableLinks.", row.Id);
            }

            // 3) Delete the file row itself
            ctx.Files.Remove(row);
            await ctx.SaveChangesAsync();
            log.Information("Removed file DB row for FileId={FileId}.", row.Id);

            // 4) Decrement pooled ref or delete physical file
            try
            {
                if (!string.IsNullOrWhiteSpace(pooledHash))
                {
                    await BlobRefHelper.DecrementBlobRefCountAsync(ctx, pooledHash, 1, log);
                }
                else
                {
                    TryDeletePhysical(physical, log);
                }
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to finalize blob / physical delete for FileId={FileId}.", row.Id);
            }
        }

        // ======================= HARD DELETE FOLDER SUBTREE =======================

        private static async Task HardDeleteFolderSubtreeAsync(
            CloudStorageDbContext ctx,
            int folderId,
            string userId,
            string ip,
            string opId,
            ILogger baseLog,
            string userRootPhysical)
        {
            var log = baseLog.ForContext("Action", "HardDeleteFolder").ForContext("FolderId", folderId);

            var rootPathKey = await ctx.Folders.AsNoTracking().Where(f => f.Id == folderId).Select(f => f.PathKey).FirstOrDefaultAsync();
            var subtreeIds = !string.IsNullOrWhiteSpace(rootPathKey)
                ? await GetSubtreeFolderIdsByPathKeyAsync(ctx, rootPathKey)
                : await GetSubtreeFolderIdsBfsAsync(ctx, folderId);

            // Capture PHYSICAL paths BEFORE altering folder rows (skip 'Bin')
            var physicalPaths = new List<string>();
            foreach (var fid in subtreeIds.OrderByDescending(i => i))
            {
                var phys = await GetPhysicalPathForFolderAsync(ctx, fid, userRootPhysical);
                if (!string.IsNullOrWhiteSpace(phys))
                    physicalPaths.Add(phys);
            }

            // Files first
            var files = await ctx.Files.Where(f => subtreeIds.Contains(f.FolderId)).ToListAsync();
            log.Information("Subtree hard delete: Files={Files}, Folders={Folders}", files.Count, subtreeIds.Count);

            foreach (var fm in files)
            {
                try
                {
                    fm.PendingDelete = true;
                    fm.FinalizationStage = "Deleting";
                    await ctx.SaveChangesAsync();
                    await HardDeleteSingleBinFileAsync(ctx, fm, userId, ip, opId, log);
                }
                catch (Exception ex)
                {
                    log.Warning(ex, "Failed deleting file in subtree FileId={Id}", fm.Id);
                }
            }

            // Remove folder rows bottom-up
            foreach (var fid in subtreeIds.OrderByDescending(i => i))
            {
                var folder = await ctx.Folders.FirstOrDefaultAsync(f => f.Id == fid);
                if (folder != null) ctx.Folders.Remove(folder);
            }
            await ctx.SaveChangesAsync();

            // First pass physical deletion
            foreach (var dir in physicalPaths)
            {
                await SafeDirectoryDeletion.TryDeleteRecursiveAsync(dir, log);
            }

            // Second pass: remove any leftover empty directories (some may not have been captured if path resolver mismatch)
            await SafeDeleteDirectorySetAsync(physicalPaths, log);

            log.Information("Hard delete subtree complete FolderId={FolderId}", folderId);
        }

        private static async Task SafeDeleteDirectorySetAsync(IEnumerable<string> dirs, ILogger log)
        {
            foreach (var dir in dirs.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                try
                {
                    if (IODirectory.Exists(dir) && !IODirectory.EnumerateFileSystemEntries(dir).Any())
                    {
                        IODirectory.Delete(dir, false);
                        log.Information("Post-pass removed empty dir {Dir}", dir);
                    }
                }
                catch (Exception ex)
                {
                    log.Warning(ex, "Post-pass failed dir={Dir}", dir);
                }
            }
            await Task.CompletedTask;
        }

        private static async Task MarkFolderTreePendingDeleteAsync(CloudStorageDbContext ctx, int folderId, ILogger log)
        {
            var rootPathKey = await ctx.Folders.AsNoTracking().Where(f => f.Id == folderId).Select(f => f.PathKey).FirstOrDefaultAsync();
            var subtreeIds = !string.IsNullOrWhiteSpace(rootPathKey)
                ? await GetSubtreeFolderIdsByPathKeyAsync(ctx, rootPathKey)
                : await GetSubtreeFolderIdsBfsAsync(ctx, folderId);

            var files = await ctx.Files.Where(f => subtreeIds.Contains(f.FolderId)).ToListAsync();
            foreach (var f in files)
            {
                f.PendingDelete = true;
                f.FinalizationStage = "Deleting";
            }
            await ctx.SaveChangesAsync();
            log.Information("Marked subtree pending delete FolderId={FolderId} FilesMarked={Count}", folderId, files.Count);
        }

        // ======================= POOLING HELPERS =======================

        private static async Task EnsurePooledAndRepointAllAsync(CloudStorageDbContext ctx, FileModel anyRow, string userId, string ip, string opId, ILogger log)
        {
            if (string.IsNullOrWhiteSpace(anyRow.FileHash)) return;

            var hash = anyRow.FileHash;
            var poolPath = GetSharedPoolFilePath(hash);

            var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == hash);
            if (blob == null)
            {
                var src = ResolvePhysicalPathSafe(anyRow.FilePath);
                LongPathEnsureDir(Path.GetDirectoryName(poolPath));

                if (!IOFile.Exists(poolPath))
                {
                    if (!string.IsNullOrWhiteSpace(src) && IOFile.Exists(src))
                    {
                        try { IOFile.Move(src, poolPath); }
                        catch
                        {
                            IOFile.Copy(src, poolPath, overwrite: false);
                            try { IOFile.Delete(src); } catch { }
                        }
                    }
                }

                long size = 0;
                try { size = new FileInfo(poolPath).Length; } catch { }

                blob = new FileBlob
                {
                    FileHash = hash,
                    BlobPath = poolPath,
                    Size = size,
                    CreatedAt = DateTime.UtcNow,
                    RefCount = 0,
                    Pooled = true,
                    LastRefUpdatedAt = DateTime.UtcNow
                };
                ctx.FileBlobs.Add(blob);
                await ctx.SaveChangesAsync();

                log.Information("Pooled blob created Hash={Hash} Path={Path}", hash, poolPath);
            }
            else
            {
                var src = ResolvePhysicalPathSafe(anyRow.FilePath);
                if (!string.IsNullOrWhiteSpace(src) && !src.Equals(blob.BlobPath, StringComparison.OrdinalIgnoreCase) && IOFile.Exists(src))
                {
                    try { IOFile.Delete(src); } catch { }
                }
            }

            var rows = await ctx.Files.Where(f => f.FileHash == hash).ToListAsync();
            foreach (var r in rows)
            {
                r.BlobHash = hash;
                r.FilePath = blob.BlobPath;
                r.PooledAt = r.PooledAt ?? DateTime.UtcNow;
            }
            await ctx.SaveChangesAsync();

            await BlobRefHelper.UpdateBlobRefCountFromFilesAsync(ctx, hash);
            log.Information("Repointed {Count} rows to pooled blob Hash={Hash}", rows.Count, hash);
        }

        internal static class BlobRefHelper
        {
            public static async Task UpdateBlobRefCountFromFilesAsync(CloudStorageDbContext ctx, string fileHash)
            {
                if (string.IsNullOrWhiteSpace(fileHash)) return;
                var count = await ctx.Files.AsNoTracking().CountAsync(f => f.BlobHash == fileHash);
                var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
                if (blob != null)
                {
                    blob.RefCount = count;
                    blob.LastRefUpdatedAt = DateTime.UtcNow;
                    await ctx.SaveChangesAsync();
                }
            }

            public static async Task DecrementBlobRefCountAsync(CloudStorageDbContext ctx, string fileHash, int delta, ILogger log)
            {
                if (string.IsNullOrWhiteSpace(fileHash)) return;

                var blob = await ctx.FileBlobs.FirstOrDefaultAsync(b => b.FileHash == fileHash);
                if (blob == null)
                {
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
                    log.Information("Deleted last pooled blob Hash={Hash}", fileHash);
                }
            }
        }

        // ======================= FOLDER / PATH HELPERS =======================

        private static async Task<int> EnsureUserBinFolderAsync(CloudStorageDbContext ctx, int ownerUserId, string externalUserId)
        {
            var root = await ctx.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == ownerUserId);
            if (root == null)
            {
                root = new Folder { Name = externalUserId, ParentFolderId = null, OwnerUserId = ownerUserId, CreatedAt = DateTime.UtcNow };
                ctx.Folders.Add(root);
                await ctx.SaveChangesAsync();
            }
            var bin = await ctx.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == root.Id && f.Name == "Bin");
            if (bin == null)
            {
                bin = new Folder { Name = "Bin", ParentFolderId = root.Id, OwnerUserId = ownerUserId, CreatedAt = DateTime.UtcNow };
                ctx.Folders.Add(bin);
                await ctx.SaveChangesAsync();
            }
            return bin.Id;
        }

        private static async Task<List<int>> GetSubtreeFolderIdsByPathKeyAsync(CloudStorageDbContext ctx, string rootPathKey)
            => await ctx.Folders.AsNoTracking().Where(f => f.PathKey.StartsWith(rootPathKey)).Select(f => f.Id).ToListAsync();

        private static async Task<List<int>> GetSubtreeFolderIdsBfsAsync(CloudStorageDbContext ctx, int rootFolderId)
        {
            var result = new List<int>();
            var q = new Queue<int>();
            q.Enqueue(rootFolderId);
            while (q.Count > 0)
            {
                var id = q.Dequeue();
                result.Add(id);
                var children = await ctx.Folders.AsNoTracking().Where(f => f.ParentFolderId == id).Select(f => f.Id).ToListAsync();
                foreach (var c in children) q.Enqueue(c);
            }
            return result;
        }

        private static async Task<List<Folder>> GetDescendantFoldersAsync(CloudStorageDbContext ctx, int rootFolderId)
        {
            var list = new List<Folder>();
            var queue = new Queue<int>();
            queue.Enqueue(rootFolderId);
            while (queue.Count > 0)
            {
                var id = queue.Dequeue();
                var children = await ctx.Folders.AsNoTracking().Where(f => f.ParentFolderId == id).ToListAsync();
                list.AddRange(children);
                foreach (var c in children) queue.Enqueue(c.Id);
            }
            return list;
        }

        private static async Task<string> GetPhysicalPathForFolderAsync(CloudStorageDbContext ctx, int folderId, string userRootPhysical)
        {
            // Build physical path ignoring any “Bin” segment
            var segs = new List<string>();
            var cur = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == folderId);
            if (cur == null) return userRootPhysical;

            while (cur != null)
            {
                if (!string.Equals(cur.Name, "Bin", StringComparison.OrdinalIgnoreCase))
                    segs.Add(cur.Name);
                if (cur.ParentFolderId == null) break;
                cur = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == cur.ParentFolderId.Value);
            }

            segs.Reverse();
            // First segment should be user root folder name (externalUserId) -> skip it for physical relative path
            var rel = string.Join(Path.DirectorySeparatorChar.ToString(), segs.Skip(1));
            return string.IsNullOrWhiteSpace(rel) ? userRootPhysical : Path.Combine(userRootPhysical, rel);
        }

        // ======================= PHYSICAL / LOW LEVEL =======================

        private static string GetUserBinFilesRoot(string externalUserId)
            => Path.Combine(UploadConfiguration.FinalUploadPath, externalUserId, "_bin_files");

        private static bool IsInBinFilesPath(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return false;
            var resolved = ResolvePhysicalPathSafe(path) ?? "";
            return resolved.IndexOf("_bin_files", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool IsSharedPoolPath(string path)
        {
            var resolved = ResolvePhysicalPathSafe(path) ?? "";
            var poolRoot = Path.Combine(UploadConfiguration.FinalUploadPath, "_shared_pool");
            return resolved.StartsWith(poolRoot, StringComparison.OrdinalIgnoreCase);
        }

        private static string GetSharedPoolFilePath(string fileHash)
        {
            var hash = (fileHash ?? "").Trim().ToLowerInvariant();
            if (string.IsNullOrEmpty(hash)) hash = "nohash";
            var shard = hash.Length >= 2 ? hash.Substring(0, 2) : hash;
            return Path.Combine(UploadConfiguration.FinalUploadPath, "_shared_pool", shard, hash, "blob");
        }

        private static string EnsureUniquePhysical(string candidate)
        {
            if (!IOFile.Exists(candidate)) return candidate;
            var dir = Path.GetDirectoryName(candidate);
            var baseName = Path.GetFileNameWithoutExtension(candidate);
            var ext = Path.GetExtension(candidate);
            int i = 1;
            string newPath;
            do
            {
                newPath = Path.Combine(dir, $"{baseName} - Copy{(i > 1 ? $" ({i})" : "")}{ext}");
                i++;
            } while (IOFile.Exists(newPath));
            return newPath;
        }

        private static void MovePhysicalFileSafe(string src, string dest, ILogger log)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(src) || !IOFile.Exists(src)) return;
                LongPathEnsureDir(Path.GetDirectoryName(dest));
                try { IOFile.Move(src, dest); }
                catch
                {
                    IOFile.Copy(src, dest, overwrite: false);
                    try { IOFile.Delete(src); } catch { }
                }
            }
            catch (Exception ex)
            {
                log.Warning(ex, "MovePhysicalFileSafe failed Src={Src} Dest={Dest}", src, dest);
            }
        }

        private static void TryDeletePhysical(string path, ILogger log)
        {
            if (string.IsNullOrWhiteSpace(path)) return;
            try
            {
                var resolved = ResolvePhysicalPathSafe(path);
                if (!string.IsNullOrWhiteSpace(resolved) && IOFile.Exists(resolved))
                    IOFile.Delete(resolved);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed physical delete Path={Path}", path);
            }
        }

        private static string ResolvePhysicalPathSafe(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (!Path.IsPathRooted(path) && (path.StartsWith("~") || path.StartsWith("/")))
            {
                try { return HostingEnvironment.MapPath(path); } catch { }
            }
            return path;
        }

        private static void LongPathEnsureDir(string dir)
        {
            if (string.IsNullOrWhiteSpace(dir)) return;
            if (!IODirectory.Exists(dir)) IODirectory.CreateDirectory(dir);
        }

        internal static class SafeDirectoryDeletion
        {
            public static async Task<bool> TryDeleteRecursiveAsync(string dir, ILogger log, int maxAttempts = 5, int delayMs = 350)
            {
                if (string.IsNullOrWhiteSpace(dir)) return true;
                if (!IODirectory.Exists(dir)) return true;

                var extended = ToExtended(dir);

                for (int attempt = 1; attempt <= maxAttempts; attempt++)
                {
                    try
                    {
                        IODirectory.Delete(extended, true);
                        if (!IODirectory.Exists(extended))
                        {
                            log.Information("Deleted directory {Dir} attempt {Attempt}", extended, attempt);
                            return true;
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Warning(ex, "Directory delete attempt {Attempt}/{Max} failed for {Dir}", attempt, maxAttempts, extended);
                    }
                    await Task.Delay(delayMs);
                }

                if (IODirectory.Exists(extended))
                {
                    log.Warning("Directory remained after retries {Dir}", extended);
                    return false;
                }
                return true;
            }

            private static string ToExtended(string path)
            {
                if (string.IsNullOrWhiteSpace(path)) return path;
                if (path.StartsWith(@"\\?\")) return path;
                if (path.StartsWith(@"\\"))
                    return @"\\?\UNC\" + path.Substring(2);
                return @"\\?\" + path;
            }
        }
    }
}