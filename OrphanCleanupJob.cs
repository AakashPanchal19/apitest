using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    public static class OrphanCleanupJob
    {
        public static async Task RunAsync()
        {
            var log = Log.ForContext("Job", "OrphanCleanup");

            using (var ctx = new CloudStorageDbContext())
            {
                // Build set of user roots (external user ids)
                var userRoots = await ctx.Users.AsNoTracking()
                    .Select(u => u.ExternalUserId)
                    .ToListAsync();

                foreach (var userId in userRoots)
                {
                    try
                    {
                        var root = Path.Combine(UploadConfiguration.FinalUploadPath, userId);
                        if (!Directory.Exists(root)) continue;

                        var validPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        // Load all folders (you can scope by OwnerUserId if needed)
                        var folders = await ctx.Folders.AsNoTracking().ToListAsync();

                        // Map each folder to physical path
                        foreach (var f in folders)
                        {
                            var physical = BuildPhysicalPathForFolder(f.Id, ctx, userId);
                            if (!string.IsNullOrWhiteSpace(physical))
                                validPaths.Add(Path.GetFullPath(physical));
                        }

                        // Find all sub dirs on disk under user root
                        foreach (var dir in EnumerateAllDirs(root))
                        {
                            // Skip shared pool & _bin_files roots (these are handled separately)
                            if (dir.IndexOf("_shared_pool", StringComparison.OrdinalIgnoreCase) >= 0) continue;
                            if (dir.EndsWith("_bin_files", StringComparison.OrdinalIgnoreCase)) continue;

                            var full = Path.GetFullPath(dir);
                            if (!validPaths.Contains(full))
                            {
                                // Candidate orphan
                                if (!Directory.EnumerateFileSystemEntries(dir).Any())
                                {
                                    try
                                    {
                                        Directory.Delete(dir, false);
                                        log.Information("Deleted orphan empty directory {Dir}", dir);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Warning(ex, "Failed deleting orphan directory {Dir}", dir);
                                    }
                                }
                                else
                                {
                                    log.Information("Orphan non-empty directory retained {Dir}", dir);
                                }
                            }
                        }

                        // Remove empty _bin_files if not used (EF-safe StartsWith without StringComparison)
                        var binFilesRoot = Path.Combine(root, "_bin_files");
                        if (Directory.Exists(binFilesRoot))
                        {
                            // EF6-friendly: single-arg StartsWith is translatable to LIKE.
                            // If your DB collation is case-insensitive (default), this is fine.
                            bool hasBinFiles = await ctx.Files.AnyAsync(f =>
                                f.IsSoftDeleted == true &&
                                f.FilePath.StartsWith(binFilesRoot) // REMOVED StringComparison overload
                            );

                            if (!hasBinFiles && !Directory.EnumerateFileSystemEntries(binFilesRoot).Any())
                            {
                                try
                                {
                                    Directory.Delete(binFilesRoot, false);
                                    log.Information("Removed unused _bin_files at {Path}", binFilesRoot);
                                }
                                catch (Exception ex)
                                {
                                    log.Warning(ex, "Could not remove unused _bin_files at {Path}", binFilesRoot);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Warning(ex, "OrphanCleanup iteration failed for user root {UserRoot}", userId);
                    }
                }
            }
        }

        private static IEnumerable<string> EnumerateAllDirs(string root)
        {
            var stack = new Stack<string>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                var cur = stack.Pop();
                yield return cur;
                try
                {
                    foreach (var d in Directory.GetDirectories(cur))
                        stack.Push(d);
                }
                catch { }
            }
        }

        private static string BuildPhysicalPathForFolder(int folderId, CloudStorageDbContext ctx, string externalUserId)
        {
            var folder = ctx.Folders.AsNoTracking().FirstOrDefault(f => f.Id == folderId);
            if (folder == null) return null;

            var rootBase = Path.Combine(UploadConfiguration.FinalUploadPath, externalUserId);

            // Reconstruct by walking parents to get names
            var segs = new List<string>();
            var cur = folder;
            while (cur != null && cur.ParentFolderId != null)
            {
                segs.Add(cur.Name);
                cur = ctx.Folders.AsNoTracking().FirstOrDefault(f => f.Id == cur.ParentFolderId.Value);
            }
            segs.Reverse();
            return segs.Count == 0 ? rootBase : Path.Combine(rootBase, Path.Combine(segs.ToArray()));
        }
    }
}