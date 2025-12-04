using BOBDrive.Models;
using BOBDrive.Services.FileOps;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BOBDrive.Services.Maintenance
{
    public static class BinRetentionJob
    {
        // Deletes files in ALL users' Bin older than retentionDays. Called by a single recurring job.
        public static async Task PurgeAsync(int retentionDays = 90, int batchSize = 500)
        {
            var log = Log.ForContext("Job", "BinRetention")
                         .ForContext("RetentionDays", retentionDays);

            var cutoffUtc = DateTime.UtcNow.AddDays(-Math.Abs(retentionDays));
            int totalDeleted = 0, binsScanned = 0;

            try
            {
                using (var db = new CloudStorageDbContext())
                {
                    // Find all Bin folders (child of a root, name == "Bin")
                    var binIds = await db.Folders.AsNoTracking()
                        .Where(f => f.Name == "Bin" && f.ParentFolderId != null)
                        .Select(f => f.Id)
                        .ToListAsync();

                    foreach (var binId in binIds)
                    {
                        binsScanned++;
                        var subtree = await GetFolderSubtreeIdsAsync(db, binId);
                        if (subtree.Count == 0) continue;

                        while (true)
                        {
                            var oldFiles = await db.Files
                                .Where(fl => subtree.Contains(fl.FolderId) && fl.UploadedAt < cutoffUtc)
                                .OrderBy(fl => fl.Id)
                                .Take(batchSize)
                                .ToListAsync();

                            if (oldFiles.Count == 0) break;

                            foreach (var f in oldFiles)
                            {
                                try
                                {
                                    // Check if this is the last reference to the physical file
                                    var physicalPath = ResolvePhysicalPathSafe(f.FilePath);
                                    if (!string.IsNullOrWhiteSpace(physicalPath))
                                    {
                                        var refCount = await db.Files.AsNoTracking().CountAsync(other => other.FilePath == f.FilePath && other.Id != f.Id);
                                        if (refCount == 0 && LongPathIO.FileExists(physicalPath))
                                        {
                                            LongPathIO.Delete(physicalPath);
                                            log.Information("Purged physical file (last reference): {Path}", physicalPath);
                                        }
                                    }
                                    db.Files.Remove(f);
                                    totalDeleted++;
                                }
                                catch (Exception ex)
                                {
                                    log.Warning(ex, "Failed to delete file Id={FileId} during Bin purge.", f.Id);
                                }
                            }
                            await db.SaveChangesAsync();
                        }

                        // Clean empty direct children under Bin (DB only; disk clean-up is best-effort elsewhere)
                        await RemoveEmptyDirectChildrenAsync(db, binId);
                    }
                }

                log.Information("BinRetention purge done. BinsScanned={Bins}, FilesDeleted={Deleted}, Cutoff={CutoffUtc}",
                    binsScanned, totalDeleted, cutoffUtc);
            }
            catch (Exception ex)
            {
                log.Error(ex, "BinRetentionJob failed.");
            }
        }


        public static async Task PurgeDepartmentBinsAsync(int retentionDays = 90, int batchSize = 500)
        {
            var log = Log.ForContext("Job", "DeptBinRetention")
                         .ForContext("RetentionDays", retentionDays);

            var cutoffUtc = DateTime.UtcNow.AddDays(-Math.Abs(retentionDays));
            int totalDeleted = 0, binsScanned = 0;

            try
            {
                using (var db = new CloudStorageDbContext())
                {
                    // Department-level bins: Name="Bin", OwnerUserId IS NULL
                    var deptBinIds = await db.Folders.AsNoTracking()
                        .Where(f => f.Name == "Bin" && f.OwnerUserId == null)
                        .Select(f => f.Id)
                        .ToListAsync();

                    foreach (var binId in deptBinIds)
                    {
                        binsScanned++;
                        var subtree = await GetFolderSubtreeIdsAsync(db, binId);
                        if (subtree.Count == 0) continue;

                        while (true)
                        {
                            var oldFiles = await db.Files
                                .Where(fl => subtree.Contains(fl.FolderId)
                                             && fl.BinEnteredAt.HasValue
                                             && fl.BinEnteredAt.Value < cutoffUtc)
                                .OrderBy(fl => fl.Id)
                                .Take(batchSize)
                                .ToListAsync();

                            if (oldFiles.Count == 0) break;

                            foreach (var f in oldFiles)
                            {
                                try
                                {
                                    // This logic mirrors what you do in PurgeAsync for user bins
                                    var physicalPath = ResolvePhysicalPathSafe(f.FilePath);
                                    if (!string.IsNullOrWhiteSpace(physicalPath))
                                    {
                                        var refCount = await db.Files.AsNoTracking()
                                            .CountAsync(other => other.FilePath == f.FilePath && other.Id != f.Id);
                                        if (refCount == 0 && LongPathIO.FileExists(physicalPath))
                                        {
                                            LongPathIO.Delete(physicalPath);
                                            log.Information("Purged physical file (last reference) from dept bin: {Path}", physicalPath);
                                        }
                                    }
                                    db.Files.Remove(f);
                                    totalDeleted++;
                                }
                                catch (Exception ex)
                                {
                                    log.Warning(ex, "Failed to delete file Id={FileId} during dept Bin purge.", f.Id);
                                }
                            }
                            await db.SaveChangesAsync();
                        }

                        // Optionally clean empty direct children under dept Bin (DB only)
                        await RemoveEmptyDirectChildrenAsync(db, binId);
                    }
                }

                log.Information("DeptBinRetention purge done. DeptBinsScanned={Bins}, FilesDeleted={Deleted}, Cutoff={CutoffUtc}",
                    binsScanned, totalDeleted, cutoffUtc);
            }
            catch (Exception ex)
            {
                log.Error(ex, "PurgeDepartmentBinsAsync failed.");
            }
        }


        private static async Task<HashSet<int>> GetFolderSubtreeIdsAsync(CloudStorageDbContext db, int rootId)
        {
            var set = new HashSet<int> { rootId };
            var q = new Queue<int>();
            q.Enqueue(rootId);

            while (q.Count > 0)
            {
                var id = q.Dequeue();
                var children = await db.Folders.AsNoTracking()
                    .Where(f => f.ParentFolderId == id)
                    .Select(f => f.Id)
                    .ToListAsync();

                foreach (var c in children)
                    if (set.Add(c)) q.Enqueue(c);
            }
            return set;
        }

        private static async Task RemoveEmptyDirectChildrenAsync(CloudStorageDbContext db, int binId)
        {
            var directChildren = await db.Folders
                .Where(f => f.ParentFolderId == binId)
                .Select(f => f.Id)
                .ToListAsync();

            foreach (var childId in directChildren)
            {
                var subtree = await GetFolderSubtreeIdsAsync(db, childId);
                var hasFiles = await db.Files.AsNoTracking().AnyAsync(fl => subtree.Contains(fl.FolderId));
                if (!hasFiles)
                {
                    await DeleteFolderTreeRecordsAsync(db, childId);
                    await db.SaveChangesAsync();
                }
            }
        }

        private static async Task DeleteFolderTreeRecordsAsync(CloudStorageDbContext db, int folderId)
        {
            var children = await db.Folders.Where(f => f.ParentFolderId == folderId).ToListAsync();
            foreach (var c in children)
                await DeleteFolderTreeRecordsAsync(db, c.Id);

            var current = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId);
            if (current != null) db.Folders.Remove(current);
        }

        private static string ResolvePhysicalPathSafe(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (!Path.IsPathRooted(path) && (path.StartsWith("~") || path.StartsWith("/")))
            {
                try { return System.Web.Hosting.HostingEnvironment.MapPath(path); } catch { }
            }
            return path;
        }
    }
}