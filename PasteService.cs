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

// Alias EF file model
using FileModel = BOBDrive.Models.File;

namespace BOBDrive.Services.FileOps
{
    /// <summary>
    /// PasteService (updated):
    /// CUT (move):
    ///   - Purely logical. Only FolderId (and optionally Name if collision) is changed.
    ///   - Never touches the physical file even if not pooled.
    ///   - If file is a shared-pool blob, it stays in the pool; other references remain valid.
    /// COPY:
    ///   - Creates a new DB reference row pointing to the same physical FilePath.
    ///   - First-time reference pooling: if original is not yet pooled and has a FileHash, move the original to the shared pool and repoint all existing rows of that hash before creating the new reference.
    /// Shared pool blob path: <FinalUploadPath>/_shared_pool/{first2chars}/{fullhash}/blob
    /// Progress:
    ///   - ProcessedBytes increments by the file size logically (no disk IO for cut/copy).
    /// Folders:
    ///   - COPY: create a new root folder under destination with unique name; replicate subtree folder hierarchy logically (files get reference rows).
    ///   - CUT: re-parent selected root folders; contained files keep their existing FilePath; no physical moves.
    /// </summary>
    public class PasteService : IPasteService
    {
        private readonly IPasteProgressStore _progress;
        private readonly ILogger _log = Log.ForContext<PasteService>();

        public PasteService(IPasteProgressStore progressStore)
        {
            _progress = progressStore ?? InMemoryPasteProgressStore.Instance;
        }

        // Entry (enqueue background job)
        public async Task<string> StartAsync(string externalUserId, string mode, int destinationFolderId, int[] fileIds, int[] folderIds)
        {
            using (var db = new CloudStorageDbContext())
            {
                var destPathDisplay = await BuildFolderDisplayPathAsync(db, destinationFolderId);
                var opId = Guid.NewGuid().ToString("N");
                _progress.Init(opId, mode, destinationFolderId, destPathDisplay);

                BOBDrive.Controllers.FileController.BackgroundEnqueue(() =>
                    PasteJobs.Process(opId, externalUserId, mode, destinationFolderId, fileIds ?? Array.Empty<int>(), folderIds ?? Array.Empty<int>()));

                return opId;
            }
        }

        // Background execution
        public async Task ProcessAsync(string opId, string externalUserId, string mode, int destinationFolderId, int[] fileIds, int[] folderIds)
        {
            try
            {
                _progress.Update(opId, s => s.Stage = "Preparing");

                using (var ctx = new CloudStorageDbContext())
                {
                    var user = await ctx.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId).ConfigureAwait(false);
                    if (user == null) throw new InvalidOperationException("User not found.");

                    var userRootPhysical = Path.Combine(UploadConfiguration.FinalUploadPath, user.ExternalUserId);
                    LongPathIO.EnsureDirectory(userRootPhysical);

                    // Mode flags
                    var isCopy = string.Equals(mode, "copy", StringComparison.OrdinalIgnoreCase);
                    var isMove = !isCopy;

                    // Build work items (files only; folders handled separately)
                    var tasks = new List<WorkItem>();

                    // Loose files
                    if (fileIds?.Length > 0)
                    {
                        var fidSet = fileIds.ToList();
                        var dbFiles = await ctx.Files.AsNoTracking()
                            .Where(f => fidSet.Contains(f.Id) && !f.IsProcessing)
                            .ToListAsync().ConfigureAwait(false);

                        foreach (var f in dbFiles)
                        {
                            tasks.Add(new WorkItem
                            {
                                SourceFileId = f.Id,
                                SourceFolderId = f.FolderId,
                                SourceFilePath = ResolvePhysicalPathSafe(f.FilePath),
                                SourceFileName = f.Name,
                                Size = f.Size,
                                RelativeFolderFromRoot = null,
                                RootFolderId = null
                            });
                        }
                    }

                    // Folders -> expand for file tasks
                    var selectedRoots = new List<Folder>();
                    if (folderIds?.Length > 0)
                    {
                        foreach (var rootFolderId in folderIds)
                        {
                            var root = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == rootFolderId).ConfigureAwait(false);
                            if (root == null) continue;
                            selectedRoots.Add(root);

                            var filesInTree = await GetFolderTreeWithFilesAsync(ctx, rootFolderId).ConfigureAwait(false);
                            foreach (var (file, relativePath) in filesInTree)
                            {
                                tasks.Add(new WorkItem
                                {
                                    SourceFileId = file.Id,
                                    SourceFolderId = file.FolderId,
                                    SourceFilePath = ResolvePhysicalPathSafe(file.FilePath),
                                    SourceFileName = file.Name,
                                    Size = file.Size,
                                    RelativeFolderFromRoot = Path.GetDirectoryName(relativePath)?.Replace('\\', '/').Trim('/', '\\'),
                                    RootFolderId = rootFolderId
                                });
                            }
                        }
                    }

                    // Progress totals
                    long totalBytes = 0;
                    foreach (var w in tasks)
                    {
                        if (w.Size > 0) totalBytes += w.Size;
                        else
                        {
                            try
                            {
                                if (!string.IsNullOrWhiteSpace(w.SourceFilePath) && System.IO.File.Exists(w.SourceFilePath))
                                    totalBytes += new System.IO.FileInfo(w.SourceFilePath).Length;
                            }
                            catch { }
                        }
                    }
                    _progress.Update(opId, s =>
                    {
                        s.TotalCount = tasks.Count;
                        s.ProcessedCount = 0;
                        s.TotalBytes = totalBytes;
                        s.ProcessedBytes = 0;
                    });

                    // COPY: create new destination root folders (logical only)
                    var copyRootMap = new Dictionary<int, int>();
                    if (isCopy && selectedRoots.Any())
                    {
                        _progress.Update(opId, s => s.Stage = "Allocating folders");
                        foreach (var srcRoot in selectedRoots)
                        {
                            var uniqueName = await GenerateUniqueFolderNameAsync(ctx, destinationFolderId, srcRoot.Name).ConfigureAwait(false);
                            var newRoot = new Folder
                            {
                                Name = uniqueName,
                                ParentFolderId = destinationFolderId,
                                OwnerUserId = user.Id,
                                CreatedAt = DateTime.UtcNow
                            };
                            ctx.Folders.Add(newRoot);
                            await ctx.SaveChangesAsync().ConfigureAwait(false);
                            copyRootMap[srcRoot.Id] = newRoot.Id;
                        }
                    }

                    // MOVE: re-parent root folders (logical only)
                    if (isMove && selectedRoots.Any())
                    {
                        _progress.Update(opId, s => s.Stage = "Relocating folders");
                        foreach (var srcRoot in selectedRoots)
                        {
                            var trackedRoot = await ctx.Folders.FirstOrDefaultAsync(f => f.Id == srcRoot.Id).ConfigureAwait(false);
                            if (trackedRoot != null && trackedRoot.ParentFolderId != null)
                            {
                                trackedRoot.ParentFolderId = destinationFolderId;
                            }
                        }
                        await ctx.SaveChangesAsync().ConfigureAwait(false);
                    }

                    // File loop
                    _progress.Update(opId, s => s.Stage = isCopy ? "Linking (Copy)" : "Moving (Logical)");
                    int processed = 0;

                    foreach (var item in tasks)
                    {
                        _progress.Update(opId, s => s.CurrentItemName = item.SourceFileName);

                        // Determine target folder
                        int targetFolderId;
                        if (isCopy && item.RootFolderId.HasValue)
                        {
                            var baseRoot = copyRootMap[item.RootFolderId.Value];
                            targetFolderId = await EnsureFolderHierarchyUnderAsync(ctx, baseRoot, item.RelativeFolderFromRoot, user.Id)
                                .ConfigureAwait(false);
                        }
                        else if (isMove && item.RootFolderId.HasValue)
                        {
                            // Already re-parented root; keep existing file folder ID
                            targetFolderId = item.SourceFolderId;
                        }
                        else
                        {
                            targetFolderId = destinationFolderId;
                        }

                        long fileSize = item.Size;
                        if (fileSize <= 0)
                        {
                            try
                            {
                                if (!string.IsNullOrWhiteSpace(item.SourceFilePath) && System.IO.File.Exists(item.SourceFilePath))
                                    fileSize = new System.IO.FileInfo(item.SourceFilePath).Length;
                            }
                            catch { }
                        }

                        if (isCopy)
                        {
                            // Logical reference copy
                            var srcFile = await ctx.Files.FirstOrDefaultAsync(f => f.Id == item.SourceFileId).ConfigureAwait(false);
                            if (srcFile != null)
                            {
                                // First-time reference pooling
                                var resolved = ResolvePhysicalPathSafe(srcFile.FilePath) ?? "";
                                var pooled = IsSharedPoolPath(resolved);
                                if (!pooled && !string.IsNullOrWhiteSpace(srcFile.FileHash))
                                {
                                    var poolPath = GetSharedPoolFilePath(srcFile.FileHash);
                                    if (!System.IO.File.Exists(poolPath))
                                    {
                                        if (!string.IsNullOrWhiteSpace(resolved) && System.IO.File.Exists(resolved))
                                        {
                                            Directory.CreateDirectory(Path.GetDirectoryName(poolPath));
                                            try
                                            {
                                                System.IO.File.Move(resolved, poolPath);
                                            }
                                            catch
                                            {
                                                IOFile.Copy(resolved, poolPath, overwrite: false);
                                                try { IOFile.Delete(resolved); } catch { }
                                            }
                                        }
                                    }
                                    // Repoint ALL rows for this hash to pool
                                    var allRows = await ctx.Files.Where(f => f.FileHash == srcFile.FileHash).ToListAsync().ConfigureAwait(false);
                                    foreach (var r in allRows) r.FilePath = poolPath;
                                    await ctx.SaveChangesAsync().ConfigureAwait(false);
                                    pooled = true;
                                }

                                var finalName = await GenerateUniqueFileNameAsync(ctx, targetFolderId, srcFile.Name).ConfigureAwait(false);
                                var newRef = new FileModel
                                {
                                    Name = finalName,
                                    ContentType = srcFile.ContentType,
                                    Size = srcFile.Size,
                                    FilePath = srcFile.FilePath, // pooled blob path or original physical path
                                    FolderId = targetFolderId,
                                    UploadedAt = DateTime.UtcNow,
                                    IsProcessing = false,
                                    ProcessingProgress = 100,
                                    FileHash = srcFile.FileHash,
                                    ZipPassword = srcFile.ZipPassword,
                                    FinalizationStage = "Completed",
                                    FinalizationProgress = 100
                                };
                                ctx.Files.Add(newRef);
                                await ctx.SaveChangesAsync().ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            // MOVE (logical only): update FolderId & Name (if collision) WITHOUT touching FilePath
                            var existing = await ctx.Files.FirstOrDefaultAsync(f => f.Id == item.SourceFileId).ConfigureAwait(false);
                            if (existing != null)
                            {
                                if (existing.FolderId != targetFolderId)
                                    existing.FolderId = targetFolderId;

                                // Unique name if collision in destination
                                bool nameCollision = await ctx.Files.AsNoTracking()
                                    .AnyAsync(f => f.FolderId == targetFolderId && f.Id != existing.Id &&
                                                   f.Name.Equals(existing.Name, StringComparison.OrdinalIgnoreCase))
                                    .ConfigureAwait(false);

                                if (nameCollision)
                                {
                                    existing.Name = await GenerateUniqueFileNameAsync(ctx, targetFolderId, existing.Name)
                                        .ConfigureAwait(false);
                                }

                                existing.UploadedAt = DateTime.UtcNow; // optional reflect move timestamp
                                // DO NOT modify existing.FilePath
                                await ctx.SaveChangesAsync().ConfigureAwait(false);
                            }
                        }

                        // Progress update (logical accounting)
                        _progress.Update(opId, s =>
                        {
                            s.ProcessedCount = ++processed;
                            s.ProcessedBytes = SafeAdd(s.ProcessedBytes, fileSize);
                        });
                    }
                }

                _progress.Complete(opId);
                _progress.ScheduleCleanup(opId, TimeSpan.FromMinutes(2));
            }
            catch (Exception ex)
            {
                _progress.Fail(opId, ex.Message);
                _log.Error(ex, "PasteService.ProcessAsync failed for op {OpId}", opId);
            }
        }

        // ---------------- Internal helpers ----------------

        private static long SafeAdd(long current, long delta)
            => (delta <= 0) ? current : (current > long.MaxValue - delta ? long.MaxValue : current + delta);

        private class WorkItem
        {
            public int SourceFileId { get; set; }
            public int SourceFolderId { get; set; }
            public string SourceFilePath { get; set; }
            public string SourceFileName { get; set; }
            public long Size { get; set; }
            public string RelativeFolderFromRoot { get; set; }
            public int? RootFolderId { get; set; }
        }

        // Shared pool helpers
        private static string GetSharedPoolRoot()
            => Path.Combine(UploadConfiguration.FinalUploadPath, "_shared_pool");

        private static string GetSharedPoolFilePath(string fileHash)
        {
            var hash = (fileHash ?? "").Trim().ToLowerInvariant();
            if (string.IsNullOrEmpty(hash)) hash = "nohash";
            var shard = hash.Length >= 2 ? hash.Substring(0, 2) : hash;
            var dir = Path.Combine(GetSharedPoolRoot(), shard, hash);
            // Directory created only when pooling occurs (not here)
            return Path.Combine(dir, "blob");
        }

        private static bool IsSharedPoolPath(string path)
        {
            var resolved = ResolvePhysicalPathSafe(path) ?? "";
            return resolved.StartsWith(GetSharedPoolRoot(), StringComparison.OrdinalIgnoreCase);
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

        private static async Task<string> BuildFolderDisplayPathAsync(CloudStorageDbContext db, int folderId)
        {
            var segs = new List<string>();
            var current = await db.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == folderId);
            if (current == null) return "(unknown)";
            while (current != null)
            {
                segs.Add(current.Name);
                if (current.ParentFolderId == null) break;
                current = await db.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == current.ParentFolderId.Value);
            }
            segs.Reverse();
            return string.Join("/", segs);
        }

        private static async Task<int> EnsureFolderHierarchyUnderAsync(CloudStorageDbContext ctx, int baseFolderId, string relativePath, int ownerUserId)
        {
            if (string.IsNullOrWhiteSpace(relativePath)) return baseFolderId;
            var segments = relativePath.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);
            int current = baseFolderId;
            foreach (var raw in segments)
            {
                var name = raw.Trim();
                var existing = await ctx.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == current && f.Name == name);
                if (existing != null)
                {
                    current = existing.Id;
                }
                else
                {
                    var nf = new Folder { Name = name, ParentFolderId = current, OwnerUserId = ownerUserId, CreatedAt = DateTime.UtcNow };
                    ctx.Folders.Add(nf);
                    await ctx.SaveChangesAsync();
                    current = nf.Id;
                }
            }
            return current;
        }

        private static async Task<string> GenerateUniqueFileNameAsync(CloudStorageDbContext ctx, int folderId, string originalName)
        {
            var baseName = Path.GetFileNameWithoutExtension(originalName);
            var ext = Path.GetExtension(originalName);
            string candidate = originalName;
            int idx = 1;

            var existingNames = await ctx.Files.AsNoTracking()
                .Where(f => f.FolderId == folderId)
                .Select(f => f.Name)
                .ToListAsync();

            var existingSet = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
            while (existingSet.Contains(candidate))
            {
                candidate = $"{baseName} - Copy{(idx > 1 ? $" ({idx})" : "")}{ext}";
                idx++;
            }
            return candidate;
        }

        private static async Task<string> GenerateUniqueFolderNameAsync(CloudStorageDbContext ctx, int parentFolderId, string originalName)
        {
            string candidate = originalName;
            int idx = 1;

            var existingNames = await ctx.Folders.AsNoTracking()
                .Where(f => f.ParentFolderId == parentFolderId)
                .Select(f => f.Name)
                .ToListAsync();

            var existingSet = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
            while (existingSet.Contains(candidate))
            {
                candidate = $"{originalName} - Copy{(idx > 1 ? $" ({idx})" : "")}";
                idx++;
            }
            return candidate;
        }

        // Folder tree enumeration (relative paths exclude root folder name)
        private static async Task<List<(FileModel file, string relativePath)>> GetFolderTreeWithFilesAsync(CloudStorageDbContext context, int rootFolderId)
        {
            var result = new List<(FileModel file, string relativePath)>();
            var rootFolder = await context.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == rootFolderId);
            if (rootFolder == null) return result;

            var allDescendantFolders = await context.Database.SqlQuery<FolderDto>(
                "WITH FolderTree AS (" +
                "  SELECT Id, CAST(NULL AS nvarchar(max)) as RelativePath FROM dbo.Folders WHERE Id = @p0 " +
                "  UNION ALL " +
                "  SELECT f.Id, CAST(CASE WHEN ft.RelativePath IS NULL THEN f.Name ELSE ft.RelativePath + N'/' + f.Name END AS nvarchar(max)) " +
                "  FROM dbo.Folders f INNER JOIN FolderTree ft ON f.ParentFolderId = ft.Id " +
                ") SELECT Id, RelativePath FROM FolderTree", rootFolderId)
                .ToListAsync();

            var folderIdToRelativePath = allDescendantFolders.ToDictionary(f => f.Id, f => f.RelativePath ?? "");
            var allFolderIds = allDescendantFolders.Select(f => f.Id).ToArray();

            var allFilesInTree = await context.Files.AsNoTracking()
                .Where(f => allFolderIds.Contains(f.FolderId) && !f.IsProcessing && f.FilePath != null && f.FilePath != "")
                .ToListAsync();

            foreach (var file in allFilesInTree)
            {
                if (folderIdToRelativePath.TryGetValue(file.FolderId, out var relativeFolderPath))
                {
                    var relPath = Path.Combine(relativeFolderPath, file.Name);
                    result.Add((file, relPath));
                }
            }
            return result;
        }

        private class FolderDto
        {
            public int Id { get; set; }
            public string RelativePath { get; set; }
        }
    }
}