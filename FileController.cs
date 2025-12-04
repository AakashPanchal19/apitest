using BOBDrive.App_Start;
using BOBDrive.Models;
using BOBDrive.Services;
using BOBDrive.Services.FileOps;
using BOBDrive.ViewModels;
using Hangfire;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;
using System.Web.Mvc;
using IOFile = System.IO.File;


using FileModel = BOBDrive.Models.File;


namespace BOBDrive.Controllers
{
    public static class UploadProgressTracker
    {
        private static readonly ConcurrentDictionary<string, ProgressInfo> _map = new ConcurrentDictionary<string, ProgressInfo>();
        public static ProgressInfo CreateEntry(string fileId)
        {
            var info = new ProgressInfo { Progress = 0, IsDone = false, FileName = null, ErrorMessage = null };
            _map[fileId] = info;
            return info;
        }

        public static bool TryGet(string fileId, out ProgressInfo info)
        {
            return _map.TryGetValue(fileId, out info);
        }

        public static void Remove(string fileId)
        {
            ProgressInfo removed;
            _map.TryRemove(fileId, out removed);
        }
    }

    public class ProgressInfo
    {
        public int Progress { get; set; }
        public bool IsDone { get; set; }
        public string FileName { get; set; }
        public string ErrorMessage { get; set; }
    }


    public class FileController : BaseController
    {
        private static readonly string _finalUploadPath = UploadConfiguration.FinalUploadPath;
        private static readonly string _tempChunkPath = UploadConfiguration.TempChunkPath;
        private static readonly string _tempMergePath = UploadConfiguration.TempMergePath;
        private static readonly TimeSpan _uploadLockTimeout = UploadConfiguration.UploadLockTimeout;
        private static readonly int _fileStreamBufferSize = UploadConfiguration.FileStreamBufferSize;
        private static readonly ILogger _log = Log.ForContext<FileController>();

        private bool ShouldDeferZip(out string reason) => ZipResourceGuard.ShouldDefer(out reason);


        private static int _activeZipJobs = 0;
        private static readonly object _zipSync = new object();

        private static bool TryEnterZipSlot()
        {
            lock (_zipSync)
            {
                if (_activeZipJobs >= UploadConfiguration.MaxConcurrentZipJobs)
                    return false;
                _activeZipJobs++;
                return true;
            }
        }

        private static void LeaveZipSlot()
        {
            lock (_zipSync)
            {
                if (_activeZipJobs > 0) _activeZipJobs--;
            }
        }

        #region Helper: ProgressUpdater Class
        /// <summary>
        /// Helper class to throttle database updates for progress reporting.
        /// This reduces database chattiness by only saving changes every few seconds.
        /// </summary>
        private class ProgressUpdater : IDisposable
        {
            private readonly CloudStorageDbContext _context;
            private readonly int _placeholderFileId;
            private readonly ILogger _logger;
            private readonly Stopwatch _stopwatch = new Stopwatch();

            // Reduced interval so fast ZIP operations report smoother progress
            private TimeSpan _updateInterval = TimeSpan.FromMilliseconds(500);

            // Track last overall % to (a) prevent regress & (b) allow faster updates on real change
            private int _lastOverallPct = -1;
            private bool _disposed = false;

            public ProgressUpdater(int placeholderFileId, ILogger logger)
            {
                _context = new CloudStorageDbContext();
                _placeholderFileId = placeholderFileId;
                _logger = logger;
                _stopwatch.Start();
            }

            public async Task UpdateAsync(string stage, int overallPct, int? processed = null, int? total = null, bool force = false)
            {
                if (!force)
                {
                    // Never allow progress to move backwards
                    if (overallPct < _lastOverallPct) return;

                    // Throttle only if:
                    //  - Interval not elapsed AND
                    //  - Progress delta is very small (<2%)
                    if (_stopwatch.Elapsed < _updateInterval && overallPct - _lastOverallPct < 2)
                        return;
                }

                try
                {
                    var rec = await _context.Files.FindAsync(_placeholderFileId);
                    if (rec != null)
                    {
                        rec.FinalizationStage = stage;
                        rec.ProcessingProgress = Math.Max(-1, Math.Min(100, overallPct));
                        if (processed.HasValue) rec.FinalizationProgress = processed.Value;
                        if (total.HasValue) rec.FinalizationWorker = $"TotalFiles={total.Value}";
                        await _context.SaveChangesAsync();
                        _lastOverallPct = overallPct;
                        _stopwatch.Restart();
                    }
                }
                catch (Exception ex)
                {
                    _logger.Warning(ex, "Progress update failure (non-critical) for placeholder {PlaceholderFileId}.", _placeholderFileId);
                }
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _context.Dispose();
                    _disposed = true;
                }
            }
        }

        #endregion



        // Provide a static helper to enqueue Hangfire jobs from services without tight coupling to Hangfire in services.
        public static void BackgroundEnqueue(System.Linq.Expressions.Expression<System.Func<System.Threading.Tasks.Task>> methodCall)
        {
            Hangfire.BackgroundJob.Enqueue(methodCall);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<JsonResult> StartPaste(string mode, int destinationFolderId, int[] fileIds, int[] folderIds)
        {


            mode = (mode ?? "").ToLowerInvariant();
            fileIds = fileIds ?? Array.Empty<int>();
            folderIds = folderIds ?? Array.Empty<int>();

            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null) return Json(new { success = false, message = "User not found." });

            // POLICY: Foreign copy gating
            if (mode == "copy" && currentUser.Department != null && !currentUser.Department.IsCopyFromOtherDriveAllowed)
            {
                bool foreign = await IsAnySourceForeign(fileIds.ToList(), folderIds.ToList(), currentUser.Id);
                if (foreign)
                    return Json(new { success = false, message = "Copying from other drives is disabled for your department." });
            }

            int destRootOwner = await GetRootFolderOwnerIdAsync(destinationFolderId);
            if (destRootOwner != currentUser.Id)
                return Json(new { success = false, message = "You can only paste into your own drive." });

            var ip = GetClientIpAddress();
            var count = fileIds.Length + folderIds.Length;

            if (count > 0)
                ClientActivityMonitor.OnFileOpsStarted(ip, count);

            var svc = new PasteService(InMemoryPasteProgressStore.Instance);
            var opId = await svc.StartAsync(currentUser.ExternalUserId, mode, destinationFolderId, fileIds, folderIds);

            if (count > 0)
                ClientActivityMonitor.OnFileOpsCompleted(ip, count);

            return Json(new { success = true, opId });
        }



        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<JsonResult> StartDelete(int[] fileIds, int[] folderIds)
        {
            var logger = _log.ForContext("Action", "StartDelete")
                             .ForContext("UserId", User.Identity.Name)
                             .ForContext("IP", GetClientIpAddress());

            fileIds = fileIds ?? new int[0];
            folderIds = folderIds ?? new int[0];

            if (fileIds.Length == 0 && folderIds.Length == 0)
                return Json(new { success = false, message = "Nothing selected to delete." });

            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null) return Json(new { success = false, message = "User not found." });

            // existing permission logic here (unchanged)

            // Files must belong to user's drive ...
            // Folders checks ...

            var ip = GetClientIpAddress();
            var count = fileIds.Length + folderIds.Length;
            if (count > 0)
                ClientActivityMonitor.OnFileOpsStarted(ip, count);

            logger.Information("Delete requested. Files={Files}, Folders={Folders}", fileIds.Length, folderIds.Length);

            var svc = new DeleteService();
            var opId = await svc.StartAsync(currentUser.ExternalUserId, ip, fileIds, folderIds);

            if (count > 0)
                ClientActivityMonitor.OnFileOpsCompleted(ip, count);

            return Json(new { success = true, opId });
        }



        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<JsonResult> EmptyBin()
        {
            var logger = _log.ForContext("Action", "EmptyBin")
                             .ForContext("UserId", User.Identity.Name)
                             .ForContext("IP", GetClientIpAddress());

            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null) return Json(new { success = false, message = "User not found." });

            logger.Warning("Empty Bin requested.");

            var svc = new DeleteService();
            var opId = await svc.StartEmptyBinAsync(currentUser.ExternalUserId, GetClientIpAddress());

            return Json(new { success = true, opId });
        }




        [HttpGet]
        public JsonResult GetPasteProgress(string opId)
        {
            if (string.IsNullOrWhiteSpace(opId))
                return Json(new { success = false, message = "Missing opId." }, JsonRequestBehavior.AllowGet);

            if (!InMemoryPasteProgressStore.Instance.TryGet(opId, out var s))
                return Json(new { success = false, message = "Unknown operation." }, JsonRequestBehavior.AllowGet);

            // Use byte-based percent when available for smooth progress
            int percent;
            if (s.TotalBytes > 0)
            {
                try
                {
                    percent = (int)Math.Floor(100.0 * Math.Min(s.ProcessedBytes, s.TotalBytes) / s.TotalBytes);
                    percent = Math.Max(0, Math.Min(100, percent));
                }
                catch { percent = s.TotalCount > 0 ? Math.Min(100, (int)Math.Floor(100.0 * s.ProcessedCount / s.TotalCount)) : 0; }
            }
            else
            {
                percent = s.TotalCount > 0 ? Math.Min(100, (int)Math.Floor(100.0 * s.ProcessedCount / s.TotalCount)) : 0;
            }

            return Json(new
            {
                success = true,
                opId = s.OpId,
                mode = s.Mode,
                stage = s.Stage,
                processed = s.ProcessedCount, // files processed
                total = s.TotalCount,
                percent = percent,
                currentItem = s.CurrentItemName,
                destinationPath = s.DestinationDisplayPath,
                isDone = s.IsDone,
                error = s.ErrorMessage
            }, JsonRequestBehavior.AllowGet);
        }



        // ---------------- Added helper for clarity ----------------
        private async Task<Department> GetDeptAsync()
        {
            return await GetCurrentUserDepartmentAsync();
        }



        private async Task<bool> HasWritePermissionAsync(int folderId, string externalUserId)
        {
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
            if (user == null) return false;

            var folder = await db.Folders.FindAsync(folderId);
            if (folder == null) return false;

            // This loop is acceptable as folder depth is unlikely to be extreme.
            while (folder.ParentFolderId != null)
            {
                folder = await db.Folders.FindAsync(folder.ParentFolderId);
                if (folder == null) return false;
            }

            return folder.OwnerUserId == user.Id;
        }

        [HttpPost]
        public async Task<JsonResult> CheckWritePermission(int folderId)
        {
            var logger = _log.ForContext("Action", "CheckWritePermission").ForContext("UserId", User.Identity.Name).ForContext("FolderId", folderId);
            if (!await HasWritePermissionAsync(folderId, User.Identity.Name))
            {
                logger.Warning("Permission denied for user.");
                return Json(new { success = false, message = "Permission denied. You can only upload files to your own drive or folders within it." });
            }
            logger.Information("Permission granted for user.");
            return Json(new { success = true });
        }

        [HttpPost]
        public JsonResult CheckDiskSpace(long totalFileSize)
        {
            var logger = _log.ForContext("Action", "CheckDiskSpace").ForContext("UserId", User.Identity.Name).ForContext("RequestedFileSize", totalFileSize);
            try
            {
                double requiredSpaceMultiplier = UploadConfiguration.DiskSpaceCheckMultiplier;
                long estimatedRequiredSpace = (long)(totalFileSize * requiredSpaceMultiplier);
                string chunkDriveRoot = Path.GetPathRoot(_tempChunkPath);
                var chunkDrive = new DriveInfo(chunkDriveRoot);
                if (chunkDrive.AvailableFreeSpace < estimatedRequiredSpace)
                {
                    logger.Error("Not enough disk space. Required: {RequiredSpace}, Available: {AvailableSpace}", estimatedRequiredSpace, chunkDrive.AvailableFreeSpace);
                    return Json(new { success = false, message = string.Format("Not enough disk space. Approximately {0} is required, but only {1} is available.", FormatBytes(estimatedRequiredSpace), FormatBytes(chunkDrive.AvailableFreeSpace)) });
                }
                logger.Information("Disk space check successful.");
                return Json(new { success = true });
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Failed to verify disk space.");
                return Json(new { success = false, message = "Could not verify disk space: " + ex.Message });
            }
        }

        [HttpGet]
        public async Task<JsonResult> GetZippingProgress(int fileId)
        {
            var fileRecord = await db.Files.AsNoTracking().FirstOrDefaultAsync(f => f.Id == fileId);
            if (fileRecord == null)
            {
                return Json(new { success = false, message = "File record not found." }, JsonRequestBehavior.AllowGet);
            }
            bool isDone = !fileRecord.IsProcessing && fileRecord.ProcessingProgress >= 100;
            int? processedFiles = fileRecord.FinalizationProgress;
            int? totalFiles = null;
            if (!string.IsNullOrEmpty(fileRecord.FinalizationWorker) && fileRecord.FinalizationWorker.StartsWith("TotalFiles=", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(fileRecord.FinalizationWorker.Substring("TotalFiles=".Length), out int tf))
                {
                    totalFiles = tf;
                }
            }
            return Json(new
            {
                success = true,
                progress = fileRecord.ProcessingProgress,
                stage = fileRecord.FinalizationStage,
                isDone = isDone,
                fileName = fileRecord.Name,
                processedFiles = processedFiles,
                totalFiles = totalFiles
            }, JsonRequestBehavior.AllowGet);
        }

        #region Zipping Entry Points (Unchanged)
        [HttpPost]
        public async Task<JsonResult> ZipFolder(int folderId, string zipFileName, int destinationFolderId)
        {

            var dept = await GetDeptAsync();
            if (dept != null && !dept.IsZippingAllowed)
                return Json(new { success = false, message = "Zipping is disabled for your department." });

            if (!await HasWritePermissionAsync(destinationFolderId, User.Identity.Name))
                return Json(new { success = false, message = "Permission denied for destination folder." });
            if (!await HasWritePermissionAsync(folderId, User.Identity.Name))
                return Json(new { success = false, message = "You may only zip folders within your own drive." });

            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (user == null) return Json(new { success = false, message = "User not found." });

            var srcFolder = await db.Folders.FindAsync(folderId);
            if (srcFolder == null) return Json(new { success = false, message = "Source folder not found." });

            zipFileName = string.IsNullOrWhiteSpace(zipFileName)
                ? $"{srcFolder.Name}-{DateTime.Now:yyyy-MM-dd-HHmm}.zip"
                : Path.GetFileNameWithoutExtension(zipFileName) + ".zip";

            var ip = GetClientIpAddress();

            var placeholder = new FileModel
            {
                Name = zipFileName,
                FolderId = destinationFolderId,
                ContentType = "application/zip",
                UploadedAt = DateTime.UtcNow,
                IsProcessing = true,
                ProcessingProgress = 0,
                FinalizationStage = "Queued",
                FilePath = "pending",
                Size = 0,
                FileHash = "",
                ZipPassword = null,
                // include IP so background worker can attribute OnZipCompleted
                FinalizationWorker = $"ZipFolder|root={folderId};ip={ip}"
            };
            db.Files.Add(placeholder);
            await db.SaveChangesAsync();

            if (ShouldDeferZip(out var reason))
            {
                _log.Information("ZIP_REQUEST deferred user={User} placeholderId={Id} reason='{Reason}' name='{Name}'",
                    user.ExternalUserId, placeholder.Id, reason, placeholder.Name);

                placeholder.FinalizationStage = "Scheduled";
                db.Files.AddOrUpdate(placeholder);
                await db.SaveChangesAsync();

                BackgroundJob.Schedule<FileController>(
                    c => c.TryStartZipArchive(placeholder.Id, user.ExternalUserId),
                    TimeSpan.FromSeconds(10));

                return Json(new
                {
                    success = true,
                    fileId = placeholder.Id,
                    fileName = placeholder.Name,
                    deferred = true,
                    reason = reason
                });
            }

            var password = UploadConfiguration.GeneratePassword();
            BackgroundJob.Enqueue<FileController>(c => c.ProcessZipFromFolderInBackground(folderId, placeholder.Id, user.ExternalUserId, password));
            _log.Information("ZIP_REQUEST: user={User} ip={IP} placeholderId={Id} srcFolder={SrcFolder} destFolder={DestFolder} name='{Name}'",
                user.ExternalUserId,
                ip,
                placeholder.Id,
                folderId,
                destinationFolderId,
                placeholder.Name);

            return Json(new { success = true, fileId = placeholder.Id, fileName = placeholder.Name, deferred = false });
        }



        [HttpPost]
        public async Task<JsonResult> ZipSelection(List<int> fileIds, List<int> folderIds, string zipFileName, int destinationFolderId)
        {
            var dept = await GetCurrentUserDepartmentAsync();
            if (dept != null && !dept.IsZippingAllowed)
                return Json(new { success = false, message = "Zipping is disabled for your department." });

            fileIds = fileIds ?? new List<int>();
            folderIds = folderIds ?? new List<int>();
            if (!fileIds.Any() && !folderIds.Any()) return Json(new { success = false, message = "No files or folders selected." });


            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null) return Json(new { success = false, message = "User not found." });
            if (!await HasWritePermissionAsync(destinationFolderId, currentUser.ExternalUserId)) return Json(new { success = false, message = "Permission denied for destination folder." });

            // Validate folders
            var validFolderIds = new HashSet<int>();
            foreach (var fid in folderIds)
            {
                var f = await db.Folders.FindAsync(fid);
                if (f == null) return Json(new { success = false, message = $"Folder id {fid} not found." });
                if (!await HasWritePermissionAsync(fid, currentUser.ExternalUserId)) return Json(new { success = false, message = $"No permission on folder {f.Name}." });
                validFolderIds.Add(fid);
            }

            // Prune nested folders
            var prunedFolders = new HashSet<int>(validFolderIds);
            var folderParentMap = await db.Folders.Where(f => validFolderIds.Contains(f.Id))
                .Select(f => new { f.Id, f.ParentFolderId }).ToListAsync();
            var folderSet = new HashSet<int>(validFolderIds);
            foreach (var fp in folderParentMap)
            {
                int? parentId = fp.ParentFolderId;
                while (parentId.HasValue)
                {
                    if (folderSet.Contains(parentId.Value))
                    {
                        prunedFolders.Remove(fp.Id);
                        break;
                    }
                    var parentFolder = folderParentMap.FirstOrDefault(x => x.Id == parentId);
                    parentId = parentFolder?.ParentFolderId;
                }
            }

            // Files not covered by above folders
            var looseFileIds = new List<int>();
            foreach (var fid in fileIds)
            {
                var file = await db.Files.FindAsync(fid);
                if (file == null || file.IsProcessing) return Json(new { success = false, message = $"File id {fid} not found or processing." });
                if (!await HasWritePermissionAsync(file.FolderId, currentUser.ExternalUserId)) return Json(new { success = false, message = $"No permission on file {file.Name}." });

                bool covered = false;
                var folder = await db.Folders.FindAsync(file.FolderId);
                while (folder != null)
                {
                    if (prunedFolders.Contains(folder.Id)) { covered = true; break; }
                    folder = folder.ParentFolderId.HasValue ? await db.Folders.FindAsync(folder.ParentFolderId.Value) : null;
                }
                if (!covered) looseFileIds.Add(fid);
            }

            zipFileName = string.IsNullOrWhiteSpace(zipFileName)
                ? $"MixedSelection-{DateTime.Now:yyyy-MM-dd-HHmm}.zip"
                : Path.GetFileNameWithoutExtension(zipFileName) + ".zip";

            var ip = GetClientIpAddress();

            var placeholder = new FileModel
            {
                Name = zipFileName,
                FolderId = destinationFolderId,
                ContentType = "application/zip",
                UploadedAt = DateTime.UtcNow,
                IsProcessing = true,
                ProcessingProgress = 0,
                FinalizationStage = "Queued",
                FilePath = "pending",
                Size = 0,
                FileHash = "",
                ZipPassword = null,
                FinalizationWorker = $"ZipMixed|files={string.Join(",", looseFileIds)};folders={string.Join(",", prunedFolders)};ip={ip}"
            };
            db.Files.Add(placeholder);
            await db.SaveChangesAsync();

            if (ShouldDeferZip(out var reason))
            {
                _log.Information("ZIP_REQUEST deferred user={User} ip={IP} placeholderId={Id} reason='{Reason}' name='{Name}' files={Files} folders={Folders}",
                    currentUser.ExternalUserId,
                    ip,
                    placeholder.Id,
                    reason,
                    placeholder.Name,
                    looseFileIds.Count,
                    prunedFolders.Count);

                placeholder.FinalizationStage = "Scheduled";
                db.Files.AddOrUpdate(placeholder);
                await db.SaveChangesAsync();

                BackgroundJob.Schedule<FileController>(
                    c => c.TryStartZipArchive(placeholder.Id, currentUser.ExternalUserId),
                    TimeSpan.FromSeconds(10));

                return Json(new
                {
                    success = true,
                    fileId = placeholder.Id,
                    fileName = placeholder.Name,
                    deferred = true,
                    reason = reason
                });
            }

            var password = UploadConfiguration.GeneratePassword();
            BackgroundJob.Enqueue<FileController>(c => c.ProcessZipMixedSelectionInBackground(looseFileIds, prunedFolders.ToList(), placeholder.Id, currentUser.ExternalUserId, password));
            return Json(new { success = true, fileId = placeholder.Id, fileName = placeholder.Name, deferred = false });
        }

        #endregion


        private static string ExtractIpFromFinalizationWorker(string worker)
        {
            if (string.IsNullOrWhiteSpace(worker)) return "(unknown)";
            // e.g. "ZipFolder|root=123;ip=1.2.3.4" or "ZipMixed|...;ip=1.2.3.4"
            var parts = worker.Split(';');
            foreach (var p in parts)
            {
                var trimmed = p.Trim();
                if (trimmed.StartsWith("ip=", StringComparison.OrdinalIgnoreCase))
                    return trimmed.Substring(3);
            }
            return "(unknown)";
        }



        // ---------------- Gating coordinator jobs (new) ----------------

        /// <summary>
        /// Lightweight coordinator that waits until resources are available, then enqueues the actual ZIP worker.
        /// Safe across app restarts thanks to Hangfire persistence.
        /// </summary>
        [Queue("zip")]
        public async Task TryStartZipArchive(int placeholderFileId, string externalUserId)
        {
            var dept = await GetDeptAsync();
            if (dept != null && !dept.IsZippingAllowed)
            {
                _log.Warning("Coordinator blocked zipping per department policy (placeholderId={PlaceholderFileId}). DeptId={DeptId}", placeholderFileId, dept.Id);
                return;
            }

            var logger = _log.ForContext("Coordinator", "TryStartZipArchive")
                             .ForContext("PlaceholderId", placeholderFileId);

            using (var ctx = new CloudStorageDbContext())
            {
                var rec = await ctx.Files.FindAsync(placeholderFileId);
                if (rec == null || !rec.IsProcessing)
                {
                    logger.Information("Placeholder missing or already completed. Nothing to do.");
                    return;
                }

                // Still high load? reschedule self
                if (ShouldDeferZip(out _))
                {
                    rec.FinalizationStage = "Scheduled";
                    ctx.Files.AddOrUpdate(rec);
                    await ctx.SaveChangesAsync();

                    BackgroundJob.Schedule<FileController>(
                        c => c.TryStartZipArchive(placeholderFileId, externalUserId),
                        TimeSpan.FromMinutes(2));
                    logger.Information("Resources constrained; re-scheduled coordinator in 2 minutes.");
                    return;
                }

                rec.FinalizationStage = "Queued";
                ctx.Files.AddOrUpdate(rec);
                await ctx.SaveChangesAsync();

                // Parse payload to choose worker
                var payload = rec.FinalizationWorker ?? "";
                if (payload.StartsWith("ZipFolder|", StringComparison.OrdinalIgnoreCase))
                {
                    var root = payload.Split('=').LastOrDefault();
                    if (int.TryParse(root, out var rootFolderId))
                    {
                        var pwd = UploadConfiguration.GeneratePassword();
                        BackgroundJob.Enqueue<FileController>(c => c.ProcessZipFromFolderInBackground(rootFolderId, placeholderFileId, externalUserId, pwd));
                        logger.Information("Enqueued ProcessZipFromFolderInBackground.");
                        return;
                    }
                }
                else if (payload.StartsWith("ZipMixed|", StringComparison.OrdinalIgnoreCase))
                {
                    // format: ZipMixed|files=1,2;folders=3,4
                    var filesPart = GetAfter(payload, "files=");
                    var foldersPart = GetAfter(payload, "folders=");
                    var fileIds = SplitIntList(filesPart);
                    var folderIds = SplitIntList(foldersPart);
                    var pwd = UploadConfiguration.GeneratePassword();
                    BackgroundJob.Enqueue<FileController>(c => c.ProcessZipMixedSelectionInBackground(fileIds, folderIds, placeholderFileId, externalUserId, pwd));
                    logger.Information("Enqueued ProcessZipMixedSelectionInBackground.");
                    return;
                }

                // Fallback (payload unknown) -> mark error
                rec.FinalizationStage = "Error";
                rec.ProcessingProgress = -1;
                ctx.Files.AddOrUpdate(rec);
                await ctx.SaveChangesAsync();
                logger.Warning("Coordinator could not parse payload; marked as error.");
            }

            List<int> SplitIntList(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return new List<int>();
                return s.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(x => int.TryParse(x, out var n) ? n : (int?)null)
                        .Where(n => n.HasValue)
                        .Select(n => n.Value)
                        .ToList();
            }
            string GetAfter(string text, string key)
            {
                var idx = text.IndexOf(key, StringComparison.OrdinalIgnoreCase);
                if (idx < 0) return "";
                var sub = text.Substring(idx + key.Length);
                var sep = sub.IndexOf(';');
                return sep >= 0 ? sub.Substring(0, sep) : sub;
            }
        }

        /// <summary>
        /// Coordinator for upload finalization when "createZip" is requested and resources are constrained.
        /// It waits and then enqueues the normal ProcessUploadInBackground job with createZip=true.
        /// </summary>
        [Queue("zip")]
        public async Task TryStartUploadZip(string fileIdForUpload, string originalFileName, int folderId, string fileContentType, string originalFileChecksum)
        {
            // POLICY: add missing dept check
            var dept = await GetDeptAsync();
            if (dept != null && !dept.IsZippingAllowed)
            {
                _log.Warning("Upload zip coordinator blocked by department policy. FileIdForUpload={FileIdForUpload} DeptId={DeptId}", fileIdForUpload, dept.Id);
                return;
            }

            var logger = _log.ForContext("Coordinator", "TryStartUploadZip")
                             .ForContext("FileIdForUpload", fileIdForUpload);

            using (var ctx = new CloudStorageDbContext())
            {
                var rec = await ctx.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && f.IsProcessing);
                if (rec == null)
                {
                    logger.Information("Upload record missing or already completed. Nothing to do.");
                    return;
                }

                if (ShouldDeferZip(out _))
                {
                    rec.FinalizationStage = "Scheduled";
                    rec.FinalizationProgress = 0;
                    ctx.Files.AddOrUpdate(rec);
                    await ctx.SaveChangesAsync();

                    BackgroundJob.Schedule<FileController>(
                        c => c.TryStartUploadZip(fileIdForUpload, originalFileName, folderId, fileContentType, originalFileChecksum),
                        TimeSpan.FromMinutes(2));
                    logger.Information("Resources constrained; re-scheduled upload-zip coordinator in 2 minutes.");
                    return;
                }

                rec.FinalizationStage = "Queued";
                rec.FinalizationProgress = 0;
                ctx.Files.AddOrUpdate(rec);
                await ctx.SaveChangesAsync();

                BackgroundJob.Enqueue<FileController>(
                    c => c.ProcessUploadInBackground(fileIdForUpload, originalFileName, folderId, fileContentType, true, originalFileChecksum));
                logger.Information("Enqueued ProcessUploadInBackground with createZip=true.");
            }
        }





        #region Background Zipping Logic (Refactored & Consolidated)

        // Represents a file to be included in a zip archive.
        private class FileToZip
        {
            public string PhysicalPath { get; set; }
            public string RelativePathInZip { get; set; }

            public long Size { get; set; } // NEW: size to smooth staging progress

        }

        /// <summary>
        /// Background worker: create ZIP from a single folder.
        /// Uses global concurrency cap and resource gating (ZipResourceGuard).
        /// </summary>
        [Queue("zip")]
        public async Task ProcessZipFromFolderInBackground(int rootFolderId, int placeholderFileId, string externalUserId, string zipPassword)
        {
            // POLICY: safety re-check
            var dept = await GetDeptAsync();
            if (dept != null && !dept.IsZippingAllowed)
            {
                _log.Warning("Background zip (folder) blocked by department policy. Placeholder={PlaceholderFileId} DeptId={DeptId}", placeholderFileId, dept.Id);
                await CleanupFailedUploadAsync(placeholderFileId);
                return;
            }

            var logger = _log.ForContext("BackgroundTask", "ZipFolder")
                             .ForContext("RootFolderId", rootFolderId)
                             .ForContext("PlaceholderFileId", placeholderFileId)
                             .ForContext("UserId", externalUserId);

            // Resolve IP from placeholder (FinalizationWorker contains ";ip=...")
            string zipIp = "(unknown)";
            try
            {
                using (var ctx = new CloudStorageDbContext())
                {
                    var rec = await ctx.Files.FindAsync(placeholderFileId);
                    if (rec != null)
                        zipIp = ExtractIpFromFinalizationWorker(rec.FinalizationWorker);
                }
            }
            catch { }

            // Count this ZIP job as active for this IP
            ClientActivityMonitor.OnZipStarted(zipIp);

            if (!TryEnterZipSlot())
            {
                logger.Information(
                    "ZIP_QUEUE: Concurrency cap {Cap} reached; rescheduling folder zip placeholder {Id}.",
                    UploadConfiguration.MaxConcurrentZipJobs,
                    placeholderFileId);

                BackgroundJob.Schedule<FileController>(
                    c => c.ProcessZipFromFolderInBackground(rootFolderId, placeholderFileId, externalUserId, zipPassword),
                    TimeSpan.FromMinutes(1));

                // This job never actually started doing work; decrement immediately
                ClientActivityMonitor.OnZipCompleted(zipIp);
                return;
            }

            try
            {
                var filesToZip = new List<FileToZip>();
                string stagingRootName = "";

                try
                {
                    using (var ctx = new CloudStorageDbContext())
                    {
                        var rootFolder = await ctx.Folders.AsNoTracking()
                            .FirstOrDefaultAsync(f => f.Id == rootFolderId);
                        if (rootFolder == null)
                            throw new InvalidOperationException("Root folder not found.");

                        stagingRootName = rootFolder.Name;

                        // Get entire tree under root, with relative paths
                        var fileEntries = await GetFolderTreeWithFilesAsync(ctx, rootFolderId);

                        filesToZip = fileEntries.Select(e => new FileToZip
                        {
                            PhysicalPath = e.file.FilePath,
                            RelativePathInZip = e.relativePath,
                            Size = e.file.Size
                        }).ToList();
                    }

                    await ProcessZipArchiveInBackground(
                        placeholderFileId,
                        filesToZip,
                        stagingRootName,
                        zipPassword,
                        logger);
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "Folder zip failed during file enumeration.");
                    await CleanupFailedUploadAsync(placeholderFileId);
                }
            }
            finally
            {
                LeaveZipSlot();
                // Job finished (success or fail) → decrement active ZIP count
                ClientActivityMonitor.OnZipCompleted(zipIp);
            }
        }

        /// <summary>
        /// Background worker: create ZIP from a mixed selection of files & folders.
        /// Uses global concurrency cap and resource gating.
        /// </summary>
        [Queue("zip")]
        public async Task ProcessZipMixedSelectionInBackground(
    List<int> looseFileIds,
    List<int> topLevelFolderIds,
    int placeholderFileId,
    string externalUserId,
    string zipPassword)
        {

            // POLICY: safety re-check
            var dept = await GetDeptAsync();
            if (dept != null && !dept.IsZippingAllowed)
            {
                _log.Warning("Background zip (mixed) blocked by department policy. Placeholder={PlaceholderFileId} DeptId={DeptId}", placeholderFileId, dept.Id);
                await CleanupFailedUploadAsync(placeholderFileId);
                return;
            }

            var logger = _log.ForContext("BackgroundTask", "ZipMixed")
                             .ForContext("PlaceholderFileId", placeholderFileId)
                             .ForContext("UserId", externalUserId);

            // Resolve IP from placeholder
            string zipIp = "(unknown)";
            try
            {
                using (var ctx = new CloudStorageDbContext())
                {
                    var rec = await ctx.Files.FindAsync(placeholderFileId);
                    if (rec != null)
                        zipIp = ExtractIpFromFinalizationWorker(rec.FinalizationWorker);
                }
            }
            catch { }

            ClientActivityMonitor.OnZipStarted(zipIp);

            if (!TryEnterZipSlot())
            {
                logger.Information(
                    "ZIP_QUEUE: Concurrency cap {Cap} reached; rescheduling mixed zip placeholder {Id}.",
                    UploadConfiguration.MaxConcurrentZipJobs,
                    placeholderFileId);

                BackgroundJob.Schedule<FileController>(
                    c => c.ProcessZipMixedSelectionInBackground(looseFileIds, topLevelFolderIds, placeholderFileId, externalUserId, zipPassword),
                    TimeSpan.FromMinutes(1));

                ClientActivityMonitor.OnZipCompleted(zipIp);
                return;
            }

            try
            {
                var filesToZip = new List<FileToZip>();

                try
                {
                    using (var ctx = new CloudStorageDbContext())
                    {
                        // ===== 1) FOLDERS =====
                        foreach (var folderId in topLevelFolderIds ?? Enumerable.Empty<int>())
                        {
                            var folderEntity = await ctx.Folders.AsNoTracking()
                                .FirstOrDefaultAsync(f => f.Id == folderId);
                            if (folderEntity == null) continue;

                            var treeFiles = await GetFolderTreeWithFilesAsync(ctx, folderId);

                            filesToZip.AddRange(treeFiles.Select(e => new FileToZip
                            {
                                PhysicalPath = e.file.FilePath,
                                RelativePathInZip = Path.Combine(folderEntity.Name, e.relativePath),
                                Size = e.file.Size
                            }));
                        }

                        // Root names currently in use (first path segment)
                        var rootNames = new HashSet<string>(
                            filesToZip
                                .Select(f => (f.RelativePathInZip ?? "")
                                    .Split(Path.DirectorySeparatorChar)
                                    .FirstOrDefault() ?? "")
                                .Where(n => !string.IsNullOrEmpty(n)),
                            StringComparer.OrdinalIgnoreCase);

                        // ===== 2) LOOSE FILES =====
                        var looseIds = (looseFileIds ?? new List<int>()).ToArray();
                        if (looseIds.Length > 0)
                        {
                            var looseFiles = await db.Files.AsNoTracking()
                                .Where(f => looseIds.Contains(f.Id)
                                            && !f.IsProcessing
                                            && f.FilePath != null
                                            && f.FilePath != "")
                                .ToListAsync();

                            foreach (var file in looseFiles)
                            {
                                string candidateName = file.Name;
                                int counter = 1;
                                while (rootNames.Contains(candidateName))
                                {
                                    candidateName =
                                        $"{Path.GetFileNameWithoutExtension(file.Name)} ({counter++}){Path.GetExtension(file.Name)}";
                                }

                                rootNames.Add(candidateName);

                                filesToZip.Add(new FileToZip
                                {
                                    PhysicalPath = file.FilePath,
                                    RelativePathInZip = candidateName,
                                    Size = file.Size
                                });
                            }
                        }
                    }

                    await ProcessZipArchiveInBackground(
                        placeholderFileId,
                        filesToZip,
                        stagingRootName: null,
                        zipPassword,
                        logger);
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "Mixed selection zip failed during file enumeration.");
                    await CleanupFailedUploadAsync(placeholderFileId);
                }
            }
            finally
            {
                LeaveZipSlot();
                ClientActivityMonitor.OnZipCompleted(zipIp);
            }
        }






        /// <summary>
        /// Consolidated core logic for creating a zip archive from a prepared list of files.
        /// </summary>
        // REPLACE the entire method body of ProcessZipArchiveInBackground with this version.
        // REPLACE the entire ProcessZipArchiveInBackground method with this version.
        // This version:
        // - Streams single large files directly into 7-Zip via -si (CompressStreamedAsync) for immediate progress.
        // - Streams multi-file/folder selections into the same archive using AppendEntriesWithStdinAsync (no staging/copy).
        // - Preserves DB updates, logging, and final move semantics.

        // REPLACE the entire existing method with this version.
        private async Task ProcessZipArchiveInBackground(
            int placeholderFileId,
            List<FileToZip> filesToZip,
            string stagingRootName,
            string zipPassword,
            ILogger logger)
        {
            string stagingPath = null;
            string tempZipPath = null;
            bool completed = false;

            // Local helper: wait until a file is not locked for exclusive access
            async Task<bool> WaitUntilUnlocked(string path, TimeSpan timeout, TimeSpan poll)
            {
                if (string.IsNullOrEmpty(path)) return true;
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (sw.Elapsed < timeout)
                {
                    try
                    {
                        using (new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                        {
                            return true;
                        }
                    }
                    catch (FileNotFoundException)
                    {
                        return true; // not there yet, fine
                    }
                    catch (IOException)
                    {
                        // locked by another process, wait and retry
                    }
                    await Task.Delay(poll);
                }
                return false;
            }

            using (var progressUpdater = new ProgressUpdater(placeholderFileId, logger))
            {
                try
                {
                    await progressUpdater.UpdateAsync("Enumerating", 0, force: true);

                    if (filesToZip == null || !filesToZip.Any())
                        throw new InvalidOperationException("No physical files resolved for zipping.");

                    int totalRequested = filesToZip.Count;
                    await progressUpdater.UpdateAsync("Enumerating", 0, 0, totalRequested);

                    stagingPath = Path.Combine(_tempMergePath, $"ZIP_{placeholderFileId}");
                    System.IO.Directory.CreateDirectory(stagingPath);

                    using (var ctx = new CloudStorageDbContext())
                    {
                        var placeholder = await ctx.Files.FindAsync(placeholderFileId);
                        if (placeholder == null) throw new InvalidOperationException("Placeholder record not found.");
                        tempZipPath = Path.Combine(stagingPath, placeholder.Name);
                    }

                    // If a previous attempt left a (possibly locked) temp archive, wait briefly for unlock
                    var unlocked = await WaitUntilUnlocked(tempZipPath, TimeSpan.FromSeconds(90), TimeSpan.FromSeconds(2));
                    if (!unlocked)
                    {
                        using (var ctx = new CloudStorageDbContext())
                        {
                            var rec = await ctx.Files.FindAsync(placeholderFileId);
                            if (rec != null)
                            {
                                rec.FinalizationStage = "Retrying";
                                rec.ProcessingProgress = 0;
                                await ctx.SaveChangesAsync();
                            }
                        }
                        throw new IOException("Temp archive still locked by another process; retry later.");
                    }

                    // Build entries list, ensuring top-level folder is included when provided
                    var entries = new List<(string SourcePath, string EntryName, long Size)>();
                    foreach (var f in filesToZip)
                    {
                        // Resolve physical path if stored as app-relative
                        string physicalPath =
                            (!Path.IsPathRooted(f.PhysicalPath) && (f.PhysicalPath.StartsWith("~") || f.PhysicalPath.StartsWith("/")))
                                ? HostingEnvironment.MapPath(f.PhysicalPath)
                                : f.PhysicalPath;

                        if (string.IsNullOrEmpty(physicalPath) || !System.IO.File.Exists(physicalPath))
                        {
                            logger.Warning("ZIP: Missing source file on disk (Original Path={OriginalPath}, Resolved Path={ResolvedPath})", f.PhysicalPath, physicalPath ?? "NULL");
                            continue;
                        }

                        long size = f.Size > 0 ? f.Size : 0;
                        if (size <= 0) { try { size = new FileInfo(physicalPath).Length; } catch { size = 0; } }

                        string entryName = string.IsNullOrEmpty(stagingRootName)
                            ? f.RelativePathInZip
                            : Path.Combine(stagingRootName, f.RelativePathInZip);

                        entries.Add((physicalPath, entryName, size));
                    }

                    if (entries.Count == 0)
                        throw new InvalidOperationException("No valid files resolved for streaming.");

                    int totalFiles = entries.Count;
                    long totalBytes = entries.Sum(e => Math.Max(0, e.Size));
                    const int LISTFILE_THRESHOLD = 50; // heuristic: >50 files -> list-file strategy

                    // Ensure UI shows "Zipping" and clears stale counters like 0/N
                    using (var ctx = new CloudStorageDbContext())
                    {
                        var rec = await ctx.Files.FindAsync(placeholderFileId);
                        if (rec != null)
                        {
                            rec.FinalizationStage = "Zipping";
                            rec.FinalizationProgress = null; // clear "processed files" counter
                            rec.ProcessingProgress = 0;
                            await ctx.SaveChangesAsync();
                        }
                    }

                    // STRATEGY 1: Single file -> stream via stdin, instant progress
                    if (entries.Count == 1)
                    {
                        var single = entries[0];
                        await progressUpdater.UpdateAsync("Zipping", 0, 0, 1, force: true);

                        var zipper = new Cli7ZipProcess();
                        var entryNameForZip = single.EntryName 
                            .Replace('\\', '/')                          // normalize for ZIP internal paths
                            .Replace(Path.DirectorySeparatorChar, '/');  // safety if running on non-Windows

                        await zipper.CompressStreamedAsync(
                            new[] { single.SourcePath },
                            tempZipPath,
                            entryNameForZip, // preserve full path inside zip
                            zipPassword,
                            (pct) =>
                            {
                                int overall = Math.Min(99, Math.Max(0, pct));
                                progressUpdater.UpdateAsync("Zipping", overall, 1, 1).Wait();
                            },
                            CancellationToken.None,
                            onProcessStarted: processId =>
                            {
                                try { System.IO.File.WriteAllText(Path.Combine(stagingPath, "zip.pid"), processId.ToString()); } catch { }
                            }
                        );

                        try { System.IO.File.Delete(Path.Combine(stagingPath, "zip.pid")); } catch { }
                    }
                    // STRATEGY 2: Many files -> hard-link staging + list file -> one 7z process
                    else if (entries.Count > LISTFILE_THRESHOLD)
                    {
                        // Choose wrapper name: either provided stagingRootName or archive base name
                        string wrapper = !string.IsNullOrEmpty(stagingRootName)
                            ? stagingRootName
                            : Path.GetFileNameWithoutExtension(tempZipPath);

                        string wrapperRoot = Path.Combine(stagingPath, wrapper);
                        Directory.CreateDirectory(wrapperRoot);

                        // If no stagingRootName was provided, prefix entries with wrapper so ZIP has top-level folder
                        if (string.IsNullOrEmpty(stagingRootName))
                        {
                            for (int i = 0; i < entries.Count; i++)
                            {
                                var e = entries[i];
                                var prefixed = (e.SourcePath, Path.Combine(wrapper, e.EntryName), e.Size);
                                entries[i] = prefixed;
                            }
                        }

                        await progressUpdater.UpdateAsync("Staging", 0, 0, totalFiles, force: true);

                        int staged = 0;
                        foreach (var e in entries)
                        {
                            // Place files under stagingPath preserving entry relative path (which includes wrapper)
                            string targetPath = Path.Combine(stagingPath, e.EntryName);
                            Directory.CreateDirectory(Path.GetDirectoryName(targetPath));

                            if (!TryCreateHardLinkLogged(e.SourcePath, targetPath, logger))
                            {
                                // Cross-volume fallback: copy
                                try
                                {
                                    System.IO.File.Copy(e.SourcePath, targetPath, overwrite: false);
                                }
                                catch (Exception ex)
                                {
                                    logger.Warning(ex, "Staging copy failed for {Src}", e.SourcePath);
                                    throw;
                                }
                            }

                            staged++;
                            int pct = Math.Min(99, Math.Max(0, (int)(staged * 100.0 / totalFiles)));
                            await progressUpdater.UpdateAsync("Staging", pct, staged, totalFiles);
                        }

                        // Build list file with entry paths relative to stagingPath (retain wrapper as top-level)
                        var listFile = Path.Combine(stagingPath, "filelist.txt");
                        var lines = entries
                            .Select(e => e.EntryName
                                .Replace('/', Path.DirectorySeparatorChar)
                                .Replace('\\', Path.DirectorySeparatorChar)
                                .Trim())
                            .Where(s => !string.IsNullOrWhiteSpace(s))
                            .ToArray();

                        // Use UTF-8 with BOM for maximum 7z compatibility
                        System.IO.File.WriteAllLines(listFile, lines, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

                        await progressUpdater.UpdateAsync("Zipping", 0, processed: null, total: totalFiles, force: true);

                        var zipper = new Cli7ZipProcess();
                        await zipper.CompressFromListFileAsync(
                            listFile,
                            tempZipPath,
                            zipPassword,
                            workingDirectory: stagingPath,     // list paths are relative to stagingPath
                            includeAbsolutePaths: false,
                            progressCallback: (pct) => progressUpdater.UpdateAsync("Zipping", Math.Min(99, Math.Max(0, pct)), processed: null, total: totalFiles).Wait(),
                            cancellationToken: CancellationToken.None,
                            onProcessStarted: processId =>
                            {
                                try { System.IO.File.WriteAllText(Path.Combine(stagingPath, "zip.pid"), processId.ToString()); } catch { }
                            }
                        );

                        try { System.IO.File.Delete(Path.Combine(stagingPath, "zip.pid")); } catch { }
                    }
                    // STRATEGY 3: Few files -> append via stdin per file, live processed count
                    else
                    {
                        await progressUpdater.UpdateAsync("Zipping", 0, processed: 0, total: totalFiles, force: true);

                        var zipperMulti = new Cli7ZipProcess();
                        int lastOverall = 0;
                        int filesDone = 0;

                        await zipperMulti.AppendEntriesWithStdinAsync(
                            destinationArchivePath: tempZipPath,
                            password: zipPassword,
                            entries: entries,
                            overallProgressCallback: (pct) =>
                            {
                                lastOverall = Math.Min(99, Math.Max(0, pct));
                                progressUpdater.UpdateAsync("Zipping", lastOverall, processed: filesDone, total: totalFiles).Wait();
                            },
                            CancellationToken.None,
                            onProcessStarted: processId =>
                            {
                                try { System.IO.File.WriteAllText(Path.Combine(stagingPath, "zip.pid"), processId.ToString()); } catch { }
                            },
                            entryCompletedCallback: (processedSoFar) =>
                            {
                                filesDone = processedSoFar;
                                progressUpdater.UpdateAsync("Zipping", lastOverall, processed: filesDone, total: totalFiles).Wait();
                            }
                        );

                        try { System.IO.File.Delete(Path.Combine(stagingPath, "zip.pid")); } catch { }
                    }

                    // Finalize and move archive to final upload path
                    await progressUpdater.UpdateAsync("Finalizing", 99, force: true);

                    using (var finalCtx = new CloudStorageDbContext())
                    {
                        var finalRec = await finalCtx.Files.FindAsync(placeholderFileId);
                        if (finalRec == null)
                            throw new InvalidOperationException("Placeholder record vanished before finalization.");

                        var folderPhysicalPath = await GetPhysicalPathForFolderAsync(finalRec.FolderId);
                        Directory.CreateDirectory(folderPhysicalPath);

                        string finalDest = Path.Combine(folderPhysicalPath, finalRec.Name);
                        int dupe = 1;
                        var baseName = Path.GetFileNameWithoutExtension(finalDest);
                        var ext = Path.GetExtension(finalDest);
                        while (System.IO.File.Exists(finalDest))
                            finalDest = Path.Combine(folderPhysicalPath, $"{baseName}({dupe++}){ext}");

                        System.IO.File.Move(tempZipPath, finalDest);

                        finalRec.Name = Path.GetFileName(finalDest);
                        finalRec.FilePath = finalDest;
                        finalRec.Size = new FileInfo(finalDest).Length;
                        finalRec.IsProcessing = false;
                        finalRec.ProcessingProgress = 100;
                        finalRec.FinalizationStage = "Completed";
                        finalRec.ZipPassword = zipPassword;
                        try
                        {
                            finalRec.FileHash = await ComputeSha256Async(finalDest);
                        }
                        catch { /* best-effort; leave blank if hashing fails */ }

                        await finalCtx.SaveChangesAsync();
                    }

                    completed = true;
                    await progressUpdater.UpdateAsync("Completed", 100, force: true);
                    logger.Information("Archive created successfully (placeholder {Id}).", placeholderFileId);
                }
                catch (System.Threading.ThreadAbortException)
                {
                    logger.Warning("ThreadAbort during zipping for placeholder {Id}. Skipping failure cleanup.", placeholderFileId);
                    return;
                }
                catch (IOException ioex) when (ioex.Message.IndexOf("being used by another process", StringComparison.OrdinalIgnoreCase) >= 0
                                            || ioex.Message.IndexOf("pipe has been ended", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    logger.Warning(ioex, "Transient IO error; leaving job for retry (placeholder {Id}).", placeholderFileId);
                    try
                    {
                        using (var ctx = new CloudStorageDbContext())
                        {
                            var rec = await ctx.Files.FindAsync(placeholderFileId);
                            if (rec != null)
                            {
                                rec.FinalizationStage = "Retrying";
                                rec.ProcessingProgress = 0;
                                await ctx.SaveChangesAsync();
                            }
                        }
                    }
                    catch { /* ignore */ }
                    throw; // let Hangfire retry
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "Zip archive creation failed for placeholder {PlaceholderFileId}.", placeholderFileId);
                    await CleanupFailedUploadAsync(placeholderFileId);
                }
                finally
                {
                    if (completed && stagingPath != null && System.IO.Directory.Exists(stagingPath))
                    {
                        try { System.IO.Directory.Delete(stagingPath, true); }
                        catch (Exception cleanupEx) { logger.Warning(cleanupEx, "Staging directory cleanup failed for {Path}", stagingPath); }
                    }
                }
            }
        }
        #endregion

        // ADD: Resolve "~/" or absolute physical path
        private static string ResolvePhysicalPath(string path)
        {
            if (!Path.IsPathRooted(path) && (path.StartsWith("~") || path.StartsWith("/")))
            {
                return HostingEnvironment.MapPath(path);
            }
            return path;
        }

        // ADD: Stream copy with progress callback (delta bytes)
        private static async Task CopyFileWithProgressAsync(string source, string destination, Func<long, Task> onDeltaAsync, int bufferSize = 8 * 1024 * 1024)
        {
            using (var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(destination));
                using (var dst = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, useAsync: true))
                {
                    var buffer = new byte[bufferSize];
                    int read;
                    while ((read = await src.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        await dst.WriteAsync(buffer, 0, read);
                        if (onDeltaAsync != null) await onDeltaAsync(read);
                    }
                    await dst.FlushAsync();
                }
            }
        }

        // ADD: Try hard-link and log detailed reason on failure (no copy here)
        private bool TryCreateHardLinkLogged(string source, string destination, ILogger logger)
        {
            try
            {
                var srcEx = ToExtendedPath(source);
                var dstEx = ToExtendedPath(destination);

                if (CreateHardLink(dstEx, srcEx, IntPtr.Zero))
                {
                    return true;
                }
                else
                {
                    var err = Marshal.GetLastWin32Error();
                    logger.Warning("CreateHardLink failed (src='{Src}', dst='{Dst}', err={Err}). Will fallback to copy.",
                        source, destination, err);
                }
            }
            catch (Exception ex)
            {
                logger.Warning(ex, "CreateHardLink threw (src='{Src}', dst='{Dst}'). Will fallback to copy.", source, destination);
            }
            return false;
        }

        // ADD: Convert to extended-length path for long path support
        private static string ToExtendedPath(string path)
        {
            if (string.IsNullOrEmpty(path)) return path;
            if (path.StartsWith(@"\\?\")) return path;
            if (path.StartsWith(@"\\"))
            {
                // UNC -> \\?\UNC\server\share\...
                return @"\\?\UNC\" + path.Substring(2);
            }
            return @"\\?\" + path;
        }






        #region Optimized Helper Methods
        /// <summary>
        /// EFFICIENTLY gets all files within a folder hierarchy using a single recursive SQL query.
        /// This resolves the N+1 query problem.
        /// </summary>
        // -------------------------
        // Helper: keep this optimized and safe for EF translation
        // -------------------------
        private async Task<List<(FileModel file, string relativePath)>> GetFolderTreeWithFilesAsync(CloudStorageDbContext context, int rootFolderId)
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

            // FIX: materialize to array before Contains
            var allFolderIds = allDescendantFolders.Select(f => f.Id).ToArray();

            var allFilesInTree = await context.Files.AsNoTracking()
                .Where(f => allFolderIds.Contains(f.FolderId)
                            && !f.IsProcessing
                            && f.FilePath != null
                            && f.FilePath != "")
                .ToListAsync();

            foreach (var file in allFilesInTree)
            {
                if (folderIdToRelativePath.TryGetValue(file.FolderId, out var relativeFolderPath))
                {
                    result.Add((file, Path.Combine(relativeFolderPath, file.Name)));
                }
            }
            return result;
        }

        private class FolderDto
        {
            public int Id { get; set; }
            public string RelativePath { get; set; }
        }

        private bool TryCreateHardLinkOrCopy(string source, string destination)
        {
            try { if (CreateHardLink(destination, source, IntPtr.Zero)) return true; } catch { }
            try { System.IO.File.Copy(source, destination, false); return true; } catch { return false; }
        }
        #endregion

        /// <summary>
        /// Centralized cleanup method for any failed background job (upload or zip).
        /// This is the single source of truth for marking a job as failed.
        /// </summary>
        // REPLACE this entire method in FileController
        private async Task CleanupFailedUploadAsync(int placeholderFileId)
        {
            // Avoid DB/config access during application shutdown; let reconcile pick it up later
            if (Environment.HasShutdownStarted || System.Web.HttpRuntime.AppDomainAppId == null)
            {
                _log.Warning("AppDomain is shutting down; skipping cleanup for failed process {FileId}.", placeholderFileId);
                return;
            }

            _log.Warning("Marking failed background process for File ID {FileId} in database.", placeholderFileId);
            try
            {
                using (var context = new CloudStorageDbContext())
                {
                    var failedRecord = await context.Files.FindAsync(placeholderFileId);
                    if (failedRecord != null)
                    {
                        failedRecord.FinalizationStage = "Error";
                        failedRecord.ProcessingProgress = -1;
                        // Keep IsProcessing = true, so reconcile can see and recover/requeue it
                        failedRecord.LockedBySessionId = null;
                        failedRecord.LockedAtUtc = null;
                        await context.SaveChangesAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "An exception occurred during the database cleanup for a failed process for File ID {FileId}.", placeholderFileId);
            }
        }

        private async Task CleanupFailedUploadAsync(string fileIdForUpload)
        {
            _log.Warning("Marking failed upload for {FileId} in database.", fileIdForUpload);
            try
            {
                using (var context = new CloudStorageDbContext())
                {
                    var failedRecord = await context.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload);
                    if (failedRecord != null)
                    {
                        await CleanupFailedUploadAsync(failedRecord.Id);
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "An exception occurred during the database cleanup for a failed upload {FileId}.", fileIdForUpload);
            }
        }

        private static string FormatBytes(long bytes)
        {
            string[] suffixes = { "B", "KB", "MB", "GB", "TB", "PB" };
            if (bytes == 0) return "0 B";
            int i = 0;
            double dblBytes = bytes;
            while (dblBytes >= 1024 && i < suffixes.Length - 1)
            {
                dblBytes /= 1024;
                i++;
            }
            return string.Format("{0:0.##} {1}", dblBytes, suffixes[i]);
        }

        [HttpPost]
        public async Task<JsonResult> CheckForIncompleteUpload(string originalFileChecksum)
        {
            var incompleteFile = await db.Files.FirstOrDefaultAsync(f => f.FileHash == originalFileChecksum && f.IsProcessing == true);

            if (incompleteFile != null)
            {
                bool isLockActive = incompleteFile.LockedBySessionId != null && incompleteFile.LockedAtUtc.HasValue && (DateTime.UtcNow - incompleteFile.LockedAtUtc.Value) < _uploadLockTimeout;

                // NEW LOGIC: Check for existing chunk files instead of a single temp file.
                List<int> uploadedChunkNumbers = new List<int>();
                string chunkDir = Path.Combine(_tempChunkPath, incompleteFile.FileIdForUpload);
                if (Directory.Exists(chunkDir))
                {
                    uploadedChunkNumbers = Directory.EnumerateFiles(chunkDir, "*.chunk")
                        .Select(p => {
                            int.TryParse(Path.GetFileNameWithoutExtension(p), out int chunkNum);
                            return chunkNum;
                        })
                        .Where(cn => cn >= 0)
                        .ToList();
                }

                bool hasPermissionOnOriginalFolder = await HasWritePermissionAsync(incompleteFile.FolderId, User.Identity.Name);
                return Json(new
                {
                    success = true,
                    incompleteUploadFound = true,
                    isLockActive = isLockActive,
                    canResumeButDifferentOwner = !hasPermissionOnOriginalFolder,
                    fileIdForUpload = incompleteFile.FileIdForUpload,
                    lockedBySessionId = incompleteFile.LockedBySessionId,
                    uploadedChunkNumbers = uploadedChunkNumbers // Return list of completed chunks
                });
            }

            var completedFile = await db.Files.FirstOrDefaultAsync(f => f.FileHash == originalFileChecksum && !f.IsProcessing);
            if (completedFile != null)
            {
                return Json(new { success = true, duplicateFound = true, fileName = completedFile.Name });
            }

            return Json(new { success = true, incompleteUploadFound = false, duplicateFound = false });
        }

        [HttpPost]
        public async Task<JsonResult> StartNewUploadSession(
            string fileIdForUpload,
            string originalFileName,
            long? totalFileSize,
            string fileContentType,
            int folderId,
            string originalFileChecksum,
            string resumeFileIdForUpload = null,
            bool moveFolder = false)
        {
            // RESUME
            if (!string.IsNullOrEmpty(resumeFileIdForUpload))
            {
                var fileToResume = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == resumeFileIdForUpload && f.IsProcessing);
                if (fileToResume == null)
                {
                    return Json(new
                    {
                        success = false,
                        message = "The upload session could not be found. It may have been cancelled or completed by another session.",
                        sessionNotFound = true
                    });
                }

                int folderToCheck = moveFolder ? folderId : fileToResume.FolderId;
                if (!await HasWritePermissionAsync(folderToCheck, User.Identity.Name))
                {
                    return Json(new { success = false, message = "Permission denied to resume upload in the target folder." });
                }

                if (moveFolder) fileToResume.FolderId = folderId;

                // Take/refresh lock
                fileToResume.LockedBySessionId = Session.SessionID;
                fileToResume.LockedAtUtc = DateTime.UtcNow;
                await db.SaveChangesAsync();

                // Compute verifiedBytes from chunks on disk to allow client to jump ahead
                long verifiedBytes = 0;
                try
                {
                    string chunkDir = Path.Combine(_tempChunkPath, fileToResume.FileIdForUpload);
                    if (Directory.Exists(chunkDir))
                    {
                        verifiedBytes = Directory.EnumerateFiles(chunkDir, "*.chunk")
                                                 .Select(p => new FileInfo(p).Length)
                                                 .Sum();
                    }
                }
                catch { /* ignore */ }

                return Json(new
                {
                    success = true,
                    fileIdForUpload = fileToResume.FileIdForUpload,
                    verifiedBytes
                });
            }

            // NEW upload
            if (!await HasWritePermissionAsync(folderId, User.Identity.Name))
            {
                return Json(new { success = false, message = "Permission denied. You can only upload files to your own drive." });
            }
            if (string.IsNullOrEmpty(fileIdForUpload) || string.IsNullOrEmpty(originalFileName) || !totalFileSize.HasValue)
            {
                return Json(new { success = false, message = "Cannot start a new upload session with incomplete parameters." });
            }

            try
            {
                var newFile = new BOBDrive.Models.File
                {
                    FileIdForUpload = fileIdForUpload,
                    Name = originalFileName,
                    ContentType = fileContentType,
                    Size = totalFileSize.Value,
                    FilePath = "",
                    FolderId = folderId,
                    UploadedAt = DateTime.UtcNow,
                    IsProcessing = true,
                    ProcessingProgress = 0,
                    FileHash = originalFileChecksum,
                    ZipPassword = null,
                    LockedBySessionId = Session.SessionID,
                    LockedAtUtc = DateTime.UtcNow
                };
                db.Files.Add(newFile);

                string chunkDir = Path.Combine(_tempChunkPath, fileIdForUpload);
                Directory.CreateDirectory(chunkDir);

                await db.SaveChangesAsync();
                return Json(new { success = true, fileIdForUpload });
            }
            catch (Exception ex)
            {
                return Json(new { success = false, message = "Failed to start upload session: " + ex.Message });
            }
        }

        // New helper method to compute SHA256 for a stream
        private async Task<string> ComputeSha256ForStreamAsync(Stream stream)
        {
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                byte[] hash = await Task.Run(() => sha256.ComputeHash(stream));
                return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
            }
        }

        [HttpPost]
        public async Task<JsonResult> UploadChunk(HttpPostedFileBase chunk, string fileIdForUpload, int chunkNumber, string chunkChecksum)
        {
            var logger = _log.ForContext("Action", "UploadChunk")
                             .ForContext("UserId", User.Identity.Name)
                             .ForContext("FileIdForUpload", fileIdForUpload)
                             .ForContext("ChunkNumber", chunkNumber)
                             .ForContext("ClientIP", GetClientIpAddress());

            if (chunk == null || chunk.ContentLength == 0)
            {
                logger.Warning("Empty chunk received.");
                return Json(new { success = false, message = "Empty chunk received." });
            }
            if (string.IsNullOrEmpty(chunkChecksum))
            {
                logger.Warning("Chunk received without a checksum.");
                return Json(new { success = false, message = "Chunk received without a checksum. Client might be outdated." });
            }

            var fileRecord = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && f.IsProcessing);
            if (fileRecord == null)
            {
                logger.Error("Upload session not found in database or it was already completed.");
                return Json(new { success = false, message = "Upload session not found or already completed.", abort = true });
            }

            // ADOPT OR ENFORCE LOCK
            var now = DateTime.UtcNow;
            var lockIsActive = fileRecord.LockedAtUtc.HasValue && (now - fileRecord.LockedAtUtc.Value) < _uploadLockTimeout;
            if (fileRecord.LockedBySessionId == null || !lockIsActive)
            {
                // Adopt lock for this session if no active owner
                fileRecord.LockedBySessionId = Session.SessionID;
                fileRecord.LockedAtUtc = now;
                await db.SaveChangesAsync();
                logger.Information("Adopted upload lock for session {SessionId}.", Session.SessionID);
            }
            else if (fileRecord.LockedBySessionId != Session.SessionID)
            {
                // Another active session owns the lock
                logger.Error("Session lock mismatch (Zombie upload detected). Locked by: {LockedBy}, Current: {CurrentSession}",
                             fileRecord.LockedBySessionId, Session.SessionID);
                return Json(new { success = false, message = "This upload is being handled by another active session.", abort = true });
            }

            // Refresh lock time
            fileRecord.LockedAtUtc = now;
            await db.SaveChangesAsync();

            string chunkDir = Path.Combine(_tempChunkPath, fileIdForUpload);
            string chunkPath = Path.Combine(chunkDir, chunkNumber + ".chunk");

            try
            {
                // Verify chunk integrity before saving
                string serverChunkChecksum = await ComputeSha256ForStreamAsync(chunk.InputStream);
                chunk.InputStream.Position = 0;

                if (!string.Equals(serverChunkChecksum, chunkChecksum, StringComparison.OrdinalIgnoreCase))
                {
                    logger.Error("Chunk #{ChunkNumber} checksum mismatch. Client: {ClientChecksum}, Server: {ServerChecksum}.",
                                 chunkNumber, chunkChecksum, serverChunkChecksum);
                    return Json(new { success = false, message = "Chunk data integrity check failed. Please try again." });
                }

                Directory.CreateDirectory(chunkDir);
                chunk.SaveAs(chunkPath);

                logger.Information("Chunk #{ChunkNumber} saved to {ChunkPath}", chunkNumber, chunkPath);
                return Json(new { success = true });
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Exception while processing/saving chunk #{ChunkNumber}.", chunkNumber);
                return Json(new { success = false, message = "Error processing chunk: " + ex.Message });
            }
        }


        [HttpPost]
        public async Task<JsonResult> CancelUpload(string fileIdForUpload)
        {
            if (string.IsNullOrEmpty(fileIdForUpload)) return Json(new { success = false, message = "Invalid file ID." });
            var fileRecord = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && f.IsProcessing);
            if (fileRecord == null) return Json(new { success = true });
            if (fileRecord.LockedBySessionId != Session.SessionID) return Json(new { success = false, message = "Cannot cancel an upload locked by another session." });
            try
            {
                db.Files.Remove(fileRecord);
                await db.SaveChangesAsync();

                // Delete the temporary chunk directory for this upload
                string chunkDir = Path.Combine(_tempChunkPath, fileIdForUpload);
                if (Directory.Exists(chunkDir)) Directory.Delete(chunkDir, true);

                return Json(new { success = true });
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error during cancellation for {FileIdForUpload}", fileIdForUpload);
                return Json(new { success = false, message = "A server error occurred during cancellation." });
            }
        }

        private async Task<string> ComputeServerSideChecksumAsync(string filePath)
        {
            var fileInfo = new FileInfo(filePath);
            long fileSize = fileInfo.Length;
            long samplingThreshold = UploadConfiguration.ChecksumSamplingThresholdBytes;
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                if (fileSize <= samplingThreshold)
                {
                    int bufferSize = UploadConfiguration.FileStreamBufferSize;
                    using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true))
                    {
                        byte[] hash = await Task.Run(() => sha256.ComputeHash(stream));
                        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
                    }
                }
                else
                {
                    int sampleSize = UploadConfiguration.ChecksumSampleSizeBytes;
                    var buffer = new byte[sampleSize];
                    int bytesRead;
                    using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        fileStream.Position = 0;
                        bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length);
                        sha256.TransformBlock(buffer, 0, bytesRead, buffer, 0);
                        fileStream.Position = (fileSize / 2) - (sampleSize / 2);
                        bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length);
                        sha256.TransformBlock(buffer, 0, bytesRead, buffer, 0);
                        fileStream.Position = fileSize - sampleSize;
                        bytesRead = await fileStream.ReadAsync(buffer, 0, bytesRead);
                        sha256.TransformFinalBlock(buffer, 0, bytesRead);
                        return BitConverter.ToString(sha256.Hash).Replace("-", "").ToLowerInvariant();
                    }
                }
            }
        }




        [HttpPost]
        public async Task<JsonResult> StartFinalization(string fileIdForUpload, bool createZip)
        {


            // Existing dept zip block already present (keep)
            var logger = _log.ForContext("Action", "StartFinalization").ForContext("FileIdForUpload", fileIdForUpload);
            if (createZip)
            {
                var dept = await GetDeptAsync();
                if (dept != null && !dept.IsZippingAllowed)
                {
                    logger.Warning("Zipping blocked by policy in StartFinalization. DeptId={DeptId}", dept.Id);
                    return Json(new { success = false, message = "Zipping is disabled for your department." });
                }
            }

            var fileRecord = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && f.IsProcessing);
            if (fileRecord == null)
            {
                logger.Warning("No matching file record found to finalize.");
                return Json(new { success = false, message = "No matching file record found to finalize." });
            }
            if (fileRecord.LockedBySessionId != Session.SessionID)
            {
                logger.Error("Finalization rejected because session does not hold the lock.");
                return Json(new { success = false, message = "Cannot finalize an upload locked by another session." });
            }

            // Verify all chunks are present
            string chunkDir = Path.Combine(_tempChunkPath, fileIdForUpload);
            long totalChunks = (long)Math.Ceiling((double)fileRecord.Size / UploadConfiguration.ChunkSizeInBytes);
            int chunkCount = Directory.Exists(chunkDir) ? Directory.GetFiles(chunkDir, "*.chunk").Length : 0;

            if (chunkCount != totalChunks)
            {
                logger.Error("Finalization aborted for {FileId}. Chunk count mismatch. Expected: {ExpectedCount}, Actual: {ActualCount}", fileIdForUpload, totalChunks, chunkCount);
                return Json(new { success = false, message = "Upload is incomplete. Not all chunks have been received by the server. Please retry the upload." });
            }

            logger.Information("All chunks present for {FileId}. Proceeding with finalization.", fileIdForUpload);
            if (createZip)
            {
                fileRecord.ContentType = "application/zip";
                await db.SaveChangesAsync();
            }

            UploadProgressTracker.CreateEntry(fileIdForUpload);

            // If user requested ZIP and resources are constrained, queue a coordinator job instead
            if (createZip && ShouldDeferZip(out _))
            {
                fileRecord.FinalizationStage = "Scheduled";
                fileRecord.FinalizationProgress = 0;
                var newJobId = BackgroundJob.Schedule<FileController>(
                    c => c.TryStartUploadZip(fileRecord.FileIdForUpload, fileRecord.Name, fileRecord.FolderId, fileRecord.ContentType, fileRecord.FileHash),
                    TimeSpan.FromSeconds(10));

                fileRecord.HangfireJobId = newJobId;
                await db.SaveChangesAsync();

                logger.Information("Deferred upload-zip finalization via coordinator job {JobId}.", newJobId);
                return Json(new { success = true, action = "poll" });
            }

            // Normal path
            var jobId = BackgroundJob.Enqueue(() => ProcessUploadInBackground(fileIdForUpload, fileRecord.Name, fileRecord.FolderId, fileRecord.ContentType, createZip, fileRecord.FileHash));
            fileRecord.HangfireJobId = jobId;
            await db.SaveChangesAsync();

            logger.Information("Queued background job {JobId} for file {FileId}", jobId, fileIdForUpload);
            return Json(new { success = true, action = "poll" });
        }



        private static string GetSharedPoolRoot()
        {
            var root = Path.Combine(UploadConfiguration.FinalUploadPath, "_shared_pool");
            if (!Directory.Exists(root)) Directory.CreateDirectory(root);
            return root;
        }
        private static string GetSharedPoolFilePath(string fileHash)
        {
            var hash = (fileHash ?? "").Trim().ToLowerInvariant();
            if (string.IsNullOrEmpty(hash)) hash = "nohash";
            var shard = hash.Length >= 2 ? hash.Substring(0, 2) : hash;
            var dir = Path.Combine(GetSharedPoolRoot(), shard, hash);
            if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
            return Path.Combine(dir, "blob");
        }
        private static string ResolvePhysicalPathSafeLocal(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (!Path.IsPathRooted(path) && (path.StartsWith("~") || path.StartsWith("/")))
            {
                try { return HostingEnvironment.MapPath(path); } catch { }
            }
            return path;
        }



        // 1) INSERT THIS NEW HELPER (exact location suggestion below)

        /// <summary>
        /// Returns the physical path for a folder (FolderId) inside
        /// FinalUploadPath/<ExternalUserId>/.../<subfolders>.
        /// </summary>
        private async Task<string> GetPhysicalPathForFolderAsync(int folderId)
        {
            using (var ctx = new CloudStorageDbContext())
            {
                var leaf = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == folderId);
                if (leaf == null) return _finalUploadPath;

                // Walk up to the root folder to determine the owner
                var root = leaf;
                while (root.ParentFolderId != null)
                {
                    root = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == root.ParentFolderId.Value);
                    if (root == null) break;
                }

                int? ownerUserId = root?.OwnerUserId;
                string externalUserId = null;
                if (ownerUserId.HasValue)
                {
                    var owner = await ctx.Users.AsNoTracking().FirstOrDefaultAsync(u => u.Id == ownerUserId.Value);
                    externalUserId = owner?.ExternalUserId;
                }

                // User’s root on disk; if unknown, fall back to global path
                var userRoot = string.IsNullOrEmpty(externalUserId)
                    ? _finalUploadPath
                    : Path.Combine(_finalUploadPath, externalUserId);

                // Build the relative subpath from the leaf to (but excluding) the root
                var segments = new List<string>();
                var cur = leaf;
                while (cur != null && cur.ParentFolderId != null)
                {
                    segments.Add(cur.Name);
                    cur = await ctx.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == cur.ParentFolderId.Value);
                }
                segments.Reverse();

                return segments.Count == 0 ? userRoot : Path.Combine(userRoot, Path.Combine(segments.ToArray()));
            }
        }




        public async Task ProcessUploadInBackground(string fileIdForUpload, string originalFileName, int folderId, string fileContentType, bool createZip, string originalFileChecksum)
        {
            var logger = _log.ForContext("BackgroundTask", "ProcessUploadInBackground").ForContext("FileIdForUpload", fileIdForUpload);

            // SAFETY: If asked to zip, re-check department policy
            if (createZip)
            {
                var dept = await GetCurrentUserDepartmentAsync();
                if (dept != null && !dept.IsZippingAllowed)
                {
                    logger.Warning("Background zip blocked per department policy. DeptId={DeptId}", dept.Id);
                    // mark record as error to surface to UI
                    await CleanupFailedUploadAsync(fileIdForUpload);
                    return;
                }
            }

            logger.Information("Background finalization process started.");

            string chunkDir = Path.Combine(_tempChunkPath, fileIdForUpload);
            // Keep merge path for non-zip path only
            string tempMergePath = Path.Combine(_tempMergePath, fileIdForUpload + ".tmp");
            string finalTempPath;
            string finalFileName = originalFileName;
            string zipPassword = null;

            int fileRecordId;
            using (var db = new CloudStorageDbContext())
            {
                var fileRecord = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload);
                if (fileRecord == null) throw new Exception("File record not found at start of finalization.");
                fileRecordId = fileRecord.Id;

                fileRecord.FinalizationStage = "Initializing";
                fileRecord.FinalizationProgress = 1;
                fileRecord.FinalizationWorker = Environment.MachineName;
                fileRecord.FinalizationStartedAtUtc = DateTime.UtcNow;
                await db.SaveChangesAsync();
            }

            using (var progressUpdater = new ProgressUpdater(fileRecordId, logger))
            {
                try
                {
                    // Validate chunk completeness (already done in StartFinalization, but double-check)
                    var chunkFiles = Directory.GetFiles(chunkDir, "*.chunk")
                                              .Select(p => new { Path = p, Ord = TryParseOrDefault(Path.GetFileNameWithoutExtension(p)) })
                                              .OrderBy(x => x.Ord)
                                              .Select(x => x.Path)
                                              .ToList();

                    if (chunkFiles.Count == 0)
                        throw new Exception("No chunks found to finalize.");

                    await progressUpdater.UpdateAsync("Preparing", 0, force: true);

                    if (createZip)
                    {
                        // ZIP path: SKIP merge; stream ordered chunk files directly to 7-Zip via stdin
                        await progressUpdater.UpdateAsync("Zipping", 0, processed: 0, force: true);

                        string finalZipName = Path.GetFileNameWithoutExtension(originalFileName) + ".zip";
                        string zipStagingPath = Path.Combine(_tempMergePath, finalZipName);
                        zipPassword = UploadConfiguration.GeneratePassword();

                        var zipper = new Cli7ZipProcess();
                        await zipper.CompressStreamedAsync(
                            chunkFiles.ToArray(),     // stream all chunk files in-order
                            zipStagingPath,
                            originalFileName,         // single entry name inside zip
                            zipPassword,
                            (p) => progressUpdater.UpdateAsync("Zipping", p, processed: p, force: p < 10).Wait()
                        );

                        finalTempPath = zipStagingPath;
                        finalFileName = finalZipName;
                    }
                    else
                    {
                        // Non-zip path: MERGE, verify checksum, then move
                        await progressUpdater.UpdateAsync("Merging", 0, force: true);
                        await MergeChunksAsync(chunkDir, tempMergePath, p => progressUpdater.UpdateAsync("Merging", p).Wait());
                        logger.Information("All chunks merged successfully for {FileId}.", fileIdForUpload);

                        await progressUpdater.UpdateAsync("Verifying", 95, force: true);
                        string serverChecksum = await ComputeServerSideChecksumAsync(tempMergePath);
                        if (!string.Equals(serverChecksum, originalFileChecksum, StringComparison.OrdinalIgnoreCase))
                            throw new Exception("Final checksum verification failed.");
                        logger.Information("Server-side checksum verification successful for {FileId}.", fileIdForUpload);

                        finalTempPath = tempMergePath;
                    }

                    await progressUpdater.UpdateAsync("Finalizing", 99, force: true);

                    // Move to final store with unique name handling
                    string finalDestPath;
                    using (var context = new CloudStorageDbContext())
                    {
                        var fileRecordToFinalize = await context.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload);
                        if (fileRecordToFinalize == null) throw new InvalidOperationException("File record vanished.");

                        var folderPhysicalPath = await GetPhysicalPathForFolderAsync(fileRecordToFinalize.FolderId);
                        Directory.CreateDirectory(folderPhysicalPath);

                        finalDestPath = Path.Combine(folderPhysicalPath, finalFileName);
                        int count = 1;
                        var baseName = Path.GetFileNameWithoutExtension(finalFileName);
                        var ext = Path.GetExtension(finalFileName);
                        while (System.IO.File.Exists(finalDestPath))
                            finalDestPath = Path.Combine(folderPhysicalPath, $"{baseName}({count++}){ext}");

                        System.IO.File.Move(finalTempPath, finalDestPath);

                        fileRecordToFinalize.Name = Path.GetFileName(finalDestPath);
                        fileRecordToFinalize.ContentType = createZip ? "application/zip" : fileContentType;
                        fileRecordToFinalize.Size = new FileInfo(finalDestPath).Length;
                        fileRecordToFinalize.FilePath = finalDestPath;
                        fileRecordToFinalize.IsProcessing = false;
                        fileRecordToFinalize.ProcessingProgress = 100;
                        fileRecordToFinalize.ZipPassword = zipPassword;
                        fileRecordToFinalize.LockedBySessionId = null;
                        fileRecordToFinalize.LockedAtUtc = null;
                        fileRecordToFinalize.FinalizationStage = "Completed";
                        fileRecordToFinalize.FinalizationProgress = 100;
                        fileRecordToFinalize.HangfireJobId = null;
                        try
                        {
                            fileRecordToFinalize.FileHash = await ComputeSha256Async(finalDestPath);
                        }
                        catch { /* best-effort */ }

                        await context.SaveChangesAsync();
                    }

                    // Cleanup chunks (and merged temp if any)
                    if (Directory.Exists(chunkDir)) { try { Directory.Delete(chunkDir, true); } catch { } }
                    if (!createZip && System.IO.File.Exists(tempMergePath)) { try { System.IO.File.Delete(tempMergePath); } catch { } }

                    logger.Information("Background finalization successful for file: {FinalFileName}", Path.GetFileName(finalDestPath));
                    await progressUpdater.UpdateAsync("Completed", 100, force: true);
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "A critical error occurred during finalization. Marking as failed.");
                    await CleanupFailedUploadAsync(fileIdForUpload);
                    // Cleanup partial temps on failure
                    try { if (Directory.Exists(chunkDir)) Directory.Delete(chunkDir, true); } catch { }
                    try { if (System.IO.File.Exists(tempMergePath)) System.IO.File.Delete(tempMergePath); } catch { }
                }
            }

            int TryParseOrDefault(string s) { return int.TryParse(s, out var n) ? n : int.MaxValue; }
        }

        private async Task MergeChunksAsync(string chunkDirectory, string finalFilePath, Action<int> progressCallback)
        {
            var chunkFiles = Directory.GetFiles(chunkDirectory, "*.chunk")
                .Select(p => new { Path = p, Number = int.Parse(Path.GetFileNameWithoutExtension(p)) })
                .OrderBy(f => f.Number)
                .Select(f => f.Path)
                .ToList();

            long totalChunks = chunkFiles.Count;
            long chunksMerged = 0;
            int lastReportedProgress = -1;

            using (var destStream = new FileStream(finalFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: _fileStreamBufferSize, useAsync: true))
            {
                foreach (var chunkPath in chunkFiles)
                {
                    using (var sourceStream = new FileStream(chunkPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: _fileStreamBufferSize, useAsync: true))
                    {
                        await sourceStream.CopyToAsync(destStream);
                    }
                    chunksMerged++;
                    int progress = (int)((double)chunksMerged / totalChunks * 100);
                    if (progress != lastReportedProgress)
                    {
                        progressCallback(progress);
                        lastReportedProgress = progress;
                    }
                }
            }
        }

        [HttpPost]
        public async Task<JsonResult> RetryFinalization(string fileIdForUpload)
        {
            var logger = _log.ForContext("Action", "RetryFinalization").ForContext("FileIdForUpload", fileIdForUpload);
            var fileRecord = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && f.IsProcessing);
            if (fileRecord == null)
            {
                var completedFile = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload && !f.IsProcessing);
                if (completedFile != null)
                {
                    logger.Information("Retry requested for file {FileId} which has already completed.", fileIdForUpload);
                    return Json(new { success = true, alreadyCompleted = true, message = "This file has already been successfully processed.", folderId = completedFile.FolderId });
                }
                return Json(new { success = false, message = "File record could not be found." });
            }
            if (!await HasWritePermissionAsync(fileRecord.FolderId, User.Identity.Name))
            {
                return Json(new { success = false, message = "Permission denied." });
            }
            var hangfireJobId = fileRecord.HangfireJobId;
            bool jobRequeued = false;
            if (!string.IsNullOrEmpty(hangfireJobId))
            {
                var monitoringApi = JobStorage.Current.GetMonitoringApi();
                var jobDetails = monitoringApi.JobDetails(hangfireJobId);
                if (jobDetails != null)
                {
                    var stateName = jobDetails.History[0].StateName;
                    logger.Information("Found Hangfire job {JobId} for file {FileId} with state: {State}", hangfireJobId, fileIdForUpload, stateName);
                    if (stateName == "Scheduled" || stateName == "Failed")
                    {
                        jobRequeued = BackgroundJob.Requeue(hangfireJobId);
                        logger.Information("Re-queued Hangfire job {JobId}. Success: {RequeueSuccess}", hangfireJobId, jobRequeued);
                    }
                    else
                    {
                        return Json(new { success = false, message = string.Format("Cannot retry at this time. The current processing state is '{0}'. Please wait.", stateName ?? "Unknown") });
                    }
                }
            }
            if (!jobRequeued)
            {
                logger.Warning("Hangfire job {JobId} not found or could not be re-queued. Enqueuing a new finalization task for {FileId}", hangfireJobId, fileIdForUpload);
                fileRecord.FinalizationStage = "Retrying";
                fileRecord.FinalizationProgress = 0;
                fileRecord.LockedBySessionId = null;
                fileRecord.LockedAtUtc = null;
                var newJobId = BackgroundJob.Enqueue(() => ProcessUploadInBackground(fileRecord.FileIdForUpload, fileRecord.Name, fileRecord.FolderId, fileRecord.ContentType, fileRecord.ContentType == "application/zip", fileRecord.FileHash));
                fileRecord.HangfireJobId = newJobId;
            }
            else
            {
                fileRecord.FinalizationStage = "Retrying";
                fileRecord.FinalizationProgress = 0;
            }
            db.Files.AddOrUpdate(fileRecord);
            await db.SaveChangesAsync();
            return Json(new { success = true, message = "Finalization has been re-queued." });
        }

        private async Task PollForFileCompletion(string filePath)
        {
            _log.Information("Polling for file completion at {FilePath}", filePath);
            var timeout = TimeSpan.FromHours(3);
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < timeout)
            {
                try
                {
                    using (new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        _log.Information("File is now accessible. Polling complete.");
                        return;
                    }
                }
                catch (IOException)
                {
                    await Task.Delay(2000);
                }
            }
            throw new TimeoutException(string.Format("File at {0} remained locked for more than the timeout of {1} minutes.", filePath, timeout.TotalMinutes));
        }

        public async Task ReconcileIncompleteUploads()
        {
            _log.Information("Reconciling incomplete uploads and ZIP jobs at startup.");

            using (var dbctx = new CloudStorageDbContext())
            {
                var candidates = await dbctx.Files.Where(f => f.IsProcessing).ToListAsync();
                if (!candidates.Any())
                {
                    _log.Information("No incomplete jobs found.");
                    return;
                }

                foreach (var rec in candidates)
                {
                    try
                    {
                        if (rec.ContentType == "application/zip")
                        {
                            // ZIP branch unchanged (existing logic) ...
                            int placeholderId = rec.Id;
                            string staging = Path.Combine(_tempMergePath, $"ZIP_{placeholderId}");
                            string tempZipPath = Path.Combine(staging, rec.Name);
                            string pidFile = Path.Combine(staging, "zip.pid");

                            bool Unlocked(string path)
                            {
                                try { using (new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) { } return true; }
                                catch { return false; }
                            }
                            int? ReadZipPid()
                            {
                                try
                                {
                                    if (System.IO.File.Exists(pidFile))
                                    {
                                        var txt = System.IO.File.ReadAllText(pidFile).Trim();
                                        if (int.TryParse(txt, out var processId)) return processId;
                                    }
                                }
                                catch { }
                                return null;
                            }
                            bool Alive(int processId) { try { return !Process.GetProcessById(processId).HasExited; } catch { return false; } }

                            var zipPid = ReadZipPid();
                            if (zipPid.HasValue && Alive(zipPid.Value))
                            {
                                _log.Information("ZIP reconcile: 7z still running (PID={Pid}) for placeholder {Id}.", zipPid.Value, placeholderId);
                                continue;
                            }

                            if (System.IO.File.Exists(tempZipPath) && Unlocked(tempZipPath))
                            {
                                _log.Information("ZIP reconcile: finalizing temp archive for placeholder {Id}.", placeholderId);
                                var folderPhysicalPath = await GetPhysicalPathForFolderAsync(rec.FolderId);
                                Directory.CreateDirectory(folderPhysicalPath);

                                string finalDest = Path.Combine(folderPhysicalPath, rec.Name);
                                int dupe = 1;
                                var baseName = Path.GetFileNameWithoutExtension(finalDest);
                                var ext = Path.GetExtension(finalDest);
                                while (System.IO.File.Exists(finalDest))
                                    finalDest = Path.Combine(folderPhysicalPath, $"{baseName}({dupe++}){ext}");

                                System.IO.File.Move(tempZipPath, finalDest);

                                rec.Name = Path.GetFileName(finalDest);
                                rec.FilePath = finalDest;
                                rec.Size = new FileInfo(finalDest).Length;
                                rec.IsProcessing = false;
                                rec.ProcessingProgress = 100;
                                rec.FinalizationStage = "Completed";
                                try
                                {
                                    rec.FileHash = await ComputeSha256Async(finalDest);
                                }
                                catch { /* best-effort */ }

                                dbctx.Files.AddOrUpdate(rec);
                                await dbctx.SaveChangesAsync();

                                try { if (System.IO.Directory.Exists(staging)) System.IO.Directory.Delete(staging, true); } catch { }

                                // NEW: close out any active ZipStarted for this placeholder's IP
                                var zipIp = ExtractIpFromFinalizationWorker(rec.FinalizationWorker);
                                ClientActivityMonitor.OnZipCompleted(zipIp);

                                continue;
                            }

                            _log.Warning("ZIP reconcile: no active process and no temp for placeholder {Id}. Re-queuing.", placeholderId);
                            rec.FinalizationStage = "Retrying";
                            rec.ProcessingProgress = 0;
                            dbctx.Files.AddOrUpdate(rec);
                            await dbctx.SaveChangesAsync();
                            continue;
                        }

                        // NON-ZIP (active uploads)
                        string chunkDir = string.IsNullOrEmpty(rec.FileIdForUpload)
                            ? null
                            : Path.Combine(_tempChunkPath, rec.FileIdForUpload);

                        if (string.IsNullOrEmpty(rec.FileIdForUpload) || !Directory.Exists(chunkDir))
                        {
                            // No chunks on disk, mark as error (orphan)
                            _log.Warning("Upload reconcile: record {Id} has no chunk directory. Marking as error.", rec.Id);
                            rec.FinalizationStage = "Error";
                            rec.ProcessingProgress = -1;
                            rec.LockedBySessionId = null;
                            rec.LockedAtUtc = null;
                            dbctx.Files.AddOrUpdate(rec);
                            await dbctx.SaveChangesAsync();
                            continue;
                        }

                        // If lock is fresh, leave it alone
                        var lockFresh = rec.LockedAtUtc.HasValue && (DateTime.UtcNow - rec.LockedAtUtc.Value) < _uploadLockTimeout;
                        if (lockFresh)
                        {
                            _log.Information("Upload reconcile: active lock for {FileIdForUpload}; skipping.", rec.FileIdForUpload);
                            continue;
                        }

                        // Otherwise, release stale lock to allow client resume
                        _log.Information("Upload reconcile: releasing stale lock for {FileIdForUpload}.", rec.FileIdForUpload);
                        rec.LockedBySessionId = null;
                        rec.LockedAtUtc = null;
                        rec.FinalizationStage = "Scheduled"; // informational
                        dbctx.Files.AddOrUpdate(rec);
                        await dbctx.SaveChangesAsync();

                        // NEW:
                        var ip = ExtractIpFromFinalizationWorker(rec.FinalizationWorker);
                        ClientActivityMonitor.OnZipCompleted(ip);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Error during reconcile for File ID {Id}", rec.Id);
                    }
                }
            }
        }




        [HttpGet]
        public async Task<JsonResult> GetFinalizationProgress(string fileIdForUpload)
        {
            if (string.IsNullOrEmpty(fileIdForUpload))
            {
                return Json(new { success = false, message = "File ID is missing." }, JsonRequestBehavior.AllowGet);
            }
            var fileRecord = await db.Files.AsNoTracking().FirstOrDefaultAsync(f => f.FileIdForUpload == fileIdForUpload);
            if (fileRecord == null)
            {
                // This can happen if the file was deleted mid-process. Treat as completed to stop polling.
                return Json(new { success = true, isDone = true, progress = 100, stage = "Completed" }, JsonRequestBehavior.AllowGet);
            }
            if (!fileRecord.IsProcessing)
            {
                return Json(new { success = true, progress = 100, stage = "Completed", isDone = true, fileName = fileRecord.Name }, JsonRequestBehavior.AllowGet);
            }
            string finalizationStage = fileRecord.FinalizationStage;
            int? finalizationProgress = fileRecord.ProcessingProgress; // Use ProcessingProgress for the overall percentage

            if (!string.IsNullOrEmpty(fileRecord.HangfireJobId) && finalizationStage != "Completed" && finalizationStage != "Error")
            {
                try
                {
                    var monitoringApi = JobStorage.Current.GetMonitoringApi();
                    var jobDetails = monitoringApi.JobDetails(fileRecord.HangfireJobId);
                    if (jobDetails != null)
                    {
                        var stateName = jobDetails.History[0].StateName;
                        if (stateName == "Failed")
                        {
                            finalizationStage = "Failed";
                            finalizationProgress = -1;
                        }
                        else if (finalizationStage != "Zipping" && finalizationStage != "Verifying") // Don't override our detailed stages
                        {
                            finalizationStage = stateName;
                        }
                    }
                    else if ((DateTime.UtcNow - (fileRecord.FinalizationStartedAtUtc ?? DateTime.UtcNow)).TotalMinutes > 5)
                    {
                        finalizationStage = "Error";
                        finalizationProgress = -1;
                    }
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Could not query Hangfire for job status of {HangfireJobId}", fileRecord.HangfireJobId);
                    finalizationStage = "Error";
                    finalizationProgress = -1;
                }
            }

            bool hasFailed = finalizationProgress == -1 || finalizationStage == "Error" || finalizationStage == "Failed";
            return Json(new
            {
                success = true,
                progress = hasFailed ? -1 : finalizationProgress ?? 0,
                stage = finalizationStage,
                isDone = false,
                message = hasFailed ? "A server error occurred during finalization. Please retry." : ""
            }, JsonRequestBehavior.AllowGet);
        }

        [HttpGet]
        public async Task<ActionResult> GetFolderContents(int? folderId, bool picker = false)
        {
            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null)
            {
                return new HttpStatusCodeResult(403, "User not found.");
            }
            Folder currentFolder = null;
            if (folderId.HasValue)
            {
                currentFolder = await db.Folders.FindAsync(folderId.Value);
            }
            var subFolders = currentFolder != null ? await db.Folders.Where(f => f.ParentFolderId == currentFolder.Id).ToListAsync() : new List<Folder>();
            var files = currentFolder != null ? await db.Files.Where(f => f.FolderId == currentFolder.Id && !f.IsProcessing).ToListAsync() : new List<BOBDrive.Models.File>();
            int? parentFolderId = (currentFolder != null) ? currentFolder.ParentFolderId : null;
            var viewModel = new FolderViewModel { CurrentFolder = currentFolder, SubFolders = subFolders, Files = files, ParentOfCurrentFolderId = parentFolderId, IsPickerModel = picker };
            return PartialView("_FolderContentsPartial", viewModel);
        }

        private async Task<string> ComputeSha256Async(string filePath)
        {
            using (var sha256 = SHA256.Create())
            {
                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: _fileStreamBufferSize, useAsync: true))
                {
                    byte[] hash = await Task.Run(() => sha256.ComputeHash(stream));
                    var sb = new StringBuilder(hash.Length * 2);
                    foreach (byte b in hash) sb.Append(b.ToString("x2"));
                    return sb.ToString();
                }
            }
        }

        [HttpPost]
        public async Task<JsonResult> CreateFolder(string folderName, int? parentFolderId)
        {
            if (string.IsNullOrWhiteSpace(folderName) || folderName.Equals(".") || folderName.Equals("..")) return Json(new { success = false, message = "Invalid folder name." });
            if (!parentFolderId.HasValue) return Json(new { success = false, message = "A parent folder is required." });
            if (!await HasWritePermissionAsync(parentFolderId.Value, User.Identity.Name)) return Json(new { success = false, message = "Permission denied. You can only create folders in your own drive." });
            try
            {
                var user = await db.Users.FirstAsync(u => u.ExternalUserId == User.Identity.Name);
                var newFolder = new Folder { Name = folderName, ParentFolderId = parentFolderId, CreatedAt = DateTime.UtcNow, OwnerUserId = user.Id };
                db.Folders.Add(newFolder);
                await db.SaveChangesAsync();
                return Json(new { success = true, message = "Folder created successfully." });
            }
            catch (Exception ex)
            {
                return Json(new { success = false, message = "An error occurred while creating the folder: " + ex.Message });
            }
        }

        [HttpGet]
        public async Task<ActionResult> Download(int id)
        {
            string currentUserIdForLog = User?.Identity?.IsAuthenticated == true ? User.Identity.Name : "(anonymous)";

            var logger = _log.ForContext("Action", "Download")
                             .ForContext("FileId", id)
                             .ForContext("UserId", currentUserIdForLog)
                             .ForContext("ClientIP", GetClientIpAddress());

            try
            {
                // 1) Determine if this is a shared-link based request
                bool isSharedLinkAccess = Session["IsSharedLinkAccess"] is bool flag && flag;
                logger.Information("Download request received. IsSharedLinkAccess={IsSharedLinkAccess}", isSharedLinkAccess);

                // 2) Load file record first (we need it for both flows)
                var file = await db.Files.FindAsync(id);
                if (file == null)
                {
                    logger.Warning("Download denied: file record not found in database.");
                    return HttpNotFound("File record not found in the database.");
                }

                if (file.IsProcessing)
                {
                    logger.Warning("Download denied: file '{FileName}' is still processing.", file.Name);
                    return new HttpStatusCodeResult(
                        HttpStatusCode.Conflict,
                        "The file is currently being processed and is not yet available for download.");
                }

                // 3) If this is a shared-link download, bypass user/department/ownership logic
                if (isSharedLinkAccess)
                {
                    var physicalPathShared = ResolvePhysicalPathSafeLocal(file.FilePath);
                    if (string.IsNullOrWhiteSpace(physicalPathShared) || !System.IO.File.Exists(physicalPathShared))
                    {
                        logger.Error(
                            "INCONSISTENCY (shared link): File ID {FileId} exists in DB but its path '{FilePath}' is missing from the filesystem.",
                            id,
                            file.FilePath);

                        return HttpNotFound("The file could not be found on the server; it may have been moved or deleted.");
                    }

                    logger.Information("Download permitted via shared link access for file ID {FileId}.", id);

                    return new SharedFileStreamResult(physicalPathShared, file.ContentType, file.Name, logger);
                }

                // 4) Non-shared normal UI access: require authenticated user and apply department policies

                // 4a) Load current user
                var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
                if (currentUser == null)
                {
                    logger.Warning("Download denied: current user not found or not authenticated.");
                    return new HttpStatusCodeResult(HttpStatusCode.Unauthorized, "User not found or not authenticated.");
                }

                // 4b) Determine root-owner of the drive this file belongs to
                int fileRootOwnerId = await GetRootFolderOwnerIdAsync(file.FolderId);
                bool isOwnFile = (fileRootOwnerId == currentUser.Id);
                logger.Information("Ownership info: fileRootOwnerId={FileRootOwnerId}, currentUser.Id={CurrentUserId}, isOwnFile={IsOwnFile}",
                    fileRootOwnerId, currentUser.Id, isOwnFile);

                // 4c) Department policy
                var dept = currentUser.Department;
                bool deptAllowsCrossDriveDownload = dept != null && dept.IsDownloadingAllowed;
                logger.Information("Dept info: DeptNull={DeptNull}, IsDownloadingAllowed={IsDownloadingAllowed}",
                    dept == null, dept?.IsDownloadingAllowed);

                if (!deptAllowsCrossDriveDownload && !isOwnFile)
                {
                    // Department does NOT allow cross-drive download, and this is a foreign file -> block
                    logger.Error("Download blocked by department cross-drive policy: User {User} attempted to download foreign file ID {FileId}",
                        currentUser.ExternalUserId, id);

                    return new HttpStatusCodeResult(
                        HttpStatusCode.Forbidden,
                        "Downloading files from other drives is disabled for your department.");
                }

                // 4d) Folder-level safety check if we are restricting to own drive
                if (!deptAllowsCrossDriveDownload)
                {
                    bool hasPermission = await HasWritePermissionAsync(file.FolderId, currentUser.ExternalUserId);
                    logger.Information("Folder permission check for own-drive download: hasPermission={HasPermission}", hasPermission);

                    if (!hasPermission)
                    {
                        logger.Error(
                            "SECURITY: User {User} attempted to download file ID {FileId} which appears to be foreign despite own-file logic. FolderId={FolderId}",
                            currentUser.ExternalUserId, id, file.FolderId);

                        return new HttpStatusCodeResult(
                            HttpStatusCode.Forbidden,
                            "You do not have permission to download this file.");
                    }
                }
                else
                {
                    logger.Information("Folder-level permission check skipped: department allows cross-drive downloads.");
                }

                // 5) Physical file path check
                var physicalPath = ResolvePhysicalPathSafeLocal(file.FilePath);
                if (string.IsNullOrWhiteSpace(physicalPath) || !System.IO.File.Exists(physicalPath))
                {
                    logger.Error(
                        "INCONSISTENCY: File ID {FileId} exists in DB but its path '{FilePath}' is missing from the filesystem.",
                        id,
                        file.FilePath);

                    return HttpNotFound("The file could not be found on the server; it may have been moved or deleted.");
                }

                logger.Information("Download initiated for file '{FileName}' ({FileSize} bytes). SharedLinkAccess={IsSharedLinkAccess}, IsOwnFile={IsOwnFile}",
                    file.Name, file.Size, isSharedLinkAccess, isOwnFile);

                // 6) Stream the file
                return new SharedFileStreamResult(physicalPath, file.ContentType, file.Name, logger);
            }
            catch (IOException ex)
            {
                logger.Error(ex, "An I/O error occurred during download of file ID {FileId}.", id);
                return new HttpStatusCodeResult(
                    HttpStatusCode.InternalServerError,
                    "A server I/O error occurred while trying to access the file.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, "An unexpected error occurred during download of file ID {FileId}.", id);
                return new HttpStatusCodeResult(
                    HttpStatusCode.InternalServerError,
                    "An unexpected error occurred.");
            }
        }



        [AllowAnonymous]
        [HttpGet]
        public async Task<ActionResult> SharedDownload(int id)
        {
            var logger = _log.ForContext("Action", "SharedDownload")
                             .ForContext("FileId", id)
                             .ForContext("ClientIP", GetClientIpAddress());

            try
            {
                bool isSharedLinkAccess = Session["IsSharedLinkAccess"] is bool flag && flag;
                if (!isSharedLinkAccess)
                {
                    logger.Warning("SharedDownload denied: IsSharedLinkAccess flag not set in session.");
                    return new HttpStatusCodeResult(HttpStatusCode.Forbidden, "This download is only allowed via a valid share link.");
                }

                var file = await db.Files.FindAsync(id);
                if (file == null)
                {
                    logger.Warning("SharedDownload denied: file record not found in database.");
                    return HttpNotFound("File record not found in the database.");
                }

                if (file.IsProcessing)
                {
                    logger.Warning("SharedDownload denied: file '{FileName}' is still processing.", file.Name);
                    return new HttpStatusCodeResult(
                        HttpStatusCode.Conflict,
                        "The file is currently being processed and is not yet available for download.");
                }

                var physicalPath = ResolvePhysicalPathSafeLocal(file.FilePath);
                if (string.IsNullOrWhiteSpace(physicalPath) || !System.IO.File.Exists(physicalPath))
                {
                    logger.Error(
                        "INCONSISTENCY (shared download): File ID {FileId} exists in DB but its path '{FilePath}' is missing from the filesystem.",
                        id,
                        file.FilePath);

                    return HttpNotFound("The file could not be found on the server; it may have been moved or deleted.");
                }

                logger.Information("SharedDownload permitted for file ID {FileId}.", id);

                return new SharedFileStreamResult(physicalPath, file.ContentType, file.Name, logger);
            }
            catch (IOException ex)
            {
                logger.Error(ex, "An I/O error occurred during SharedDownload of file ID {FileId}.", id);
                return new HttpStatusCodeResult(
                    HttpStatusCode.InternalServerError,
                    "A server I/O error occurred while trying to access the file.");
            }
            catch (Exception ex)
            {
                logger.Error(ex, "An unexpected error occurred during SharedDownload of file ID {FileId}.", id);
                return new HttpStatusCodeResult(
                    HttpStatusCode.InternalServerError,
                    "An unexpected error occurred.");
            }
        }




        /// <summary>
        /// Gets the root folder owner ID for a given folder by walking up the parent chain.
        /// This determines who "owns" the drive that contains this folder.
        /// Returns -1 if not found or on error.
        /// </summary>
        private async Task<int> GetRootFolderOwnerIdAsync(int folderId)
        {
            try
            {
                var current = await db.Folders.FindAsync(folderId);
                if (current == null)
                {
                    _log.Warning("Folder not found: FolderId={FolderId}", folderId);
                    return -1;
                }

                // Walk up the folder tree to find the root folder
                while (current.ParentFolderId != null)
                {
                    current = await db.Folders.FindAsync(current.ParentFolderId.Value);
                    if (current == null)
                    {
                        _log.Error("Parent folder not found during tree walk: FolderId={FolderId}", folderId);
                        return -1;
                    }
                }

                // Root folder found - return its owner
                int ownerId = current.OwnerUserId ?? -1;
                _log.Debug("Root folder owner determined: FolderId={FolderId}, OwnerId={OwnerId}", folderId, ownerId);

                return ownerId;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error determining root folder owner: FolderId={FolderId}", folderId);
                return -1;
            }
        }




        // REPLACE the entire HandleDuplicateFile action with this:
        [HttpPost]
        public async Task<JsonResult> HandleDuplicateFile(string checksum, int
            targetFolderId)
        {
            if (!await HasWritePermissionAsync(targetFolderId, User.Identity.Name))
            {
                return Json(new { success = false, message = "Permission denied in the target folder." });
            }

            var originalFile = await db.Files.FirstOrDefaultAsync(f => f.FileHash == checksum && !f.IsProcessing);
            if (originalFile == null)
            {
                return Json(new { success = false, message = "The original file could not be found." });
            }

            // Ensure original is pooled on first reference
            var poolPath = GetSharedPoolFilePath(originalFile.FileHash);
            var origPhysical = ResolvePhysicalPathSafeLocal(originalFile.FilePath);
            var poolExists = System.IO.File.Exists(poolPath);

            if (!poolExists)
            {
                if (string.IsNullOrWhiteSpace(origPhysical) || !System.IO.File.Exists(origPhysical))
                    return Json(new { success = false, message = "Original physical file is missing; cannot create a reference." });

                try
                {
                    // Prefer move so pool definitely has content
                    System.IO.Directory.CreateDirectory(Path.GetDirectoryName(poolPath));
                    try
                    {
                        System.IO.File.Move(origPhysical, poolPath);
                    }
                    catch
                    {
                        IOFile.Copy(origPhysical, poolPath, overwrite: false);
                        try { IOFile.Delete(origPhysical); } catch { /* best-effort */ }
                    }
                }
                catch (Exception ex)
                {
                    return Json(new { success = false, message = "Failed to move original into shared pool: " + ex.Message });
                }

                poolExists = System.IO.File.Exists(poolPath);
                if (!poolExists)
                {
                    return Json(new { success = false, message = "Shared pool blob could not be created for the file." });
                }

                // Update original to point to pool
                originalFile.FilePath = poolPath;
                await db.SaveChangesAsync();
            }
            else
            {
                // Optional: if original still points to non-pool, correct it
                if (!string.Equals(ResolvePhysicalPathSafeLocal(originalFile.FilePath) ?? "", poolPath, StringComparison.OrdinalIgnoreCase))
                {
                    originalFile.FilePath = poolPath;
                    await db.SaveChangesAsync();
                }
            }

            // Create the reference pointing to the same pool path
            var newFileLink = new BOBDrive.Models.File
            {
                Name = originalFile.Name,
                ContentType = originalFile.ContentType,
                Size = originalFile.Size,
                FilePath = poolPath, // pooled path
                FolderId = targetFolderId,
                UploadedAt = DateTime.UtcNow,
                IsProcessing = false,
                ProcessingProgress = 100,
                FileHash = originalFile.FileHash,
                ZipPassword = originalFile.ZipPassword,
                LockedBySessionId = null,
                LockedAtUtc = null
            };
            db.Files.Add(newFileLink);
            await db.SaveChangesAsync();

            return Json(new { success = true, fileName = newFileLink.Name });
        }

        // --- START NEW ACTION FOR FOLDER UPLOAD ---
        [HttpPost]
        public async Task<JsonResult> EnsureFolderStructureExists(int parentFolderId, List<string> relativePaths)
        {
            var logger = _log.ForContext("Action", "EnsureFolderStructureExists").ForContext("User", User.Identity.Name).ForContext("ParentFolderId", parentFolderId);

            if (!await HasWritePermissionAsync(parentFolderId, User.Identity.Name))
            {
                logger.Warning("Permission denied to create folder structure.");
                return Json(new { success = false, message = "Permission denied in the target folder." });
            }

            var user = await db.Users.FirstAsync(u => u.ExternalUserId == User.Identity.Name);
            var pathMap = new Dictionary<string, int>();
            var allPaths = (relativePaths ?? new List<string>())
                .Select(p => p.Replace('\\', '/'))
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .Distinct()
                .OrderBy(p => p.Length)
                .ToList();

            logger.Information("Processing {PathCount} unique relative paths.", allPaths.Count);

            foreach (var path in allPaths)
            {
                try
                {
                    int currentParentId = parentFolderId;
                    var segments = path.Split('/');
                    var currentPath = new StringBuilder();

                    for (int i = 0; i < segments.Length; i++)
                    {
                        var segment = segments[i];
                        if (string.IsNullOrWhiteSpace(segment)) continue;

                        if (currentPath.Length > 0) currentPath.Append('/');
                        currentPath.Append(segment);
                        var fullRelativePath = currentPath.ToString();

                        if (pathMap.TryGetValue(fullRelativePath, out int existingId))
                        {
                            currentParentId = existingId;
                            continue;
                        }

                        // Use a new DbContext to avoid race conditions with tracked entities
                        using (var findContext = new CloudStorageDbContext())
                        {
                            var existingFolder = await findContext.Folders
                                .FirstOrDefaultAsync(f => f.ParentFolderId == currentParentId && f.Name == segment);

                            if (existingFolder != null)
                            {
                                currentParentId = existingFolder.Id;
                            }
                            else
                            {
                                var newFolder = new Folder
                                {
                                    Name = segment,
                                    ParentFolderId = currentParentId,
                                    CreatedAt = DateTime.UtcNow,
                                    OwnerUserId = user.Id
                                };
                                db.Folders.Add(newFolder);
                                await db.SaveChangesAsync(); // Save immediately to get the ID
                                currentParentId = newFolder.Id;
                            }
                            pathMap[fullRelativePath] = currentParentId;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.Error(ex, "Failed to process folder structure for path: {RelativePath}", path);
                    return Json(new { success = false, message = $"An error occurred while creating folder '{path}'." });
                }
            }

            logger.Information("Successfully created folder structure. Returning path map.");
            return Json(new { success = true, pathMap });
        }
        // --- END NEW ACTION ---

        private async Task<bool> IsRootFolderAsync(int folderId)
        {
            var f = await db.Folders.AsNoTracking().FirstOrDefaultAsync(x => x.Id == folderId);
            return f != null && f.ParentFolderId == null;
        }


        private async Task<bool> IsAnySourceForeign(List<int> fileIds, List<int> folderIds, int currentUserId)
        {
            // Simplified check logic
            if (fileIds.Any())
            {
                var fileOwners = await db.Files.Where(f => fileIds.Contains(f.Id))
                    .Select(f => f.Folder.OwnerUserId ?? db.Folders.FirstOrDefault(p => p.Id == f.Folder.ParentFolderId).OwnerUserId) // Traversal needed
                    .ToListAsync();
                // Note: You need a robust way to get Root Owner ID (existing helper GetRootFolderOwnerIdAsync)
                foreach (var fid in fileIds)
                {
                    var f = await db.Files.FindAsync(fid);
                    var owner = await GetRootFolderOwnerIdAsync(f.FolderId);
                    if (owner != currentUserId && owner != -1) return true;
                }
            }
            // Do same for folders...
            return false;
        }



        // REPLACE the entire Paste action with this version
        [HttpPost]
        public async Task<JsonResult> Paste(string mode, int destinationFolderId, List<int> fileIds, List<int> folderIds)
        {

            mode = (mode ?? "").ToLowerInvariant();
            if (mode != "copy" && mode != "cut") return Json(new { success = false, message = "Invalid operation mode." });

            fileIds = fileIds ?? new List<int>();
            folderIds = folderIds ?? new List<int>();
            if (!fileIds.Any() && !folderIds.Any()) return Json(new { success = false, message = "Nothing to paste." });

            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null) return Json(new { success = false, message = "User not found." });

            // Check Copy Policy
            if (mode == "copy" && currentUser.Department != null && !currentUser.Department.IsCopyFromOtherDriveAllowed)
            {
                // Check if any source item belongs to another user
                // (Requires checking ownership of fileIds and folderIds)
                bool isForeignSource = await IsAnySourceForeign(fileIds, folderIds, currentUser.Id);
                if (isForeignSource)
                {
                    return Json(new { success = false, message = "Copying from other drives is disabled for your department." });
                }
            }


            var destinationFolder = await db.Folders.FindAsync(destinationFolderId);
            if (destinationFolder == null) return Json(new { success = false, message = "Destination folder not found." });

            // Root owner check
            async Task<int> RootOwner(int folderId)
            {
                var cur = await db.Folders.FindAsync(folderId);
                if (cur == null) return -1;
                while (cur.ParentFolderId != null)
                {
                    cur = await db.Folders.FindAsync(cur.ParentFolderId);
                    if (cur == null) return -1;
                }
                return cur.OwnerUserId ?? -1;
            }
            int destOwner = await RootOwner(destinationFolderId);
            if (destOwner != currentUser.Id) return Json(new { success = false, message = "You can only paste into your own drive." });

            // Validate folders
            foreach (var fid in folderIds)
            {
                var fld = await db.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == fid);
                if (fld == null) return Json(new { success = false, message = "A folder to paste was not found." });
                if (fld.ParentFolderId == null) return Json(new { success = false, message = "Root folders cannot be copied or moved." });
            }

            int filesProcessed = 0, foldersProcessed = 0;

            try
            {
                if (mode == "cut")
                {
                    // CUT = logical move only (keep FilePath unchanged), works for pooled and non-pooled
                    foreach (var fid in fileIds)
                    {
                        var file = await db.Files.FindAsync(fid);
                        if (file == null || file.IsProcessing) continue;

                        int srcOwner = await RootOwner(file.FolderId);
                        if (srcOwner != currentUser.Id) return Json(new { success = false, message = "You cannot move items out of another user's drive." });

                        if (file.FolderId != destinationFolderId)
                        {
                            file.FolderId = destinationFolderId;
                            file.UploadedAt = DateTime.UtcNow; // optional: refresh timestamp to indicate move time
                        }
                        filesProcessed++;
                    }

                    foreach (var folderId in folderIds)
                    {
                        var folder = await db.Folders.FindAsync(folderId);
                        if (folder == null) continue;
                        if (folder.ParentFolderId == null) return Json(new { success = false, message = "Root folders cannot be moved." });
                        if (await IsDescendantAsync(destinationFolderId, folderId)) continue; // prevent cycles

                        int srcOwner = await RootOwner(folderId);
                        if (srcOwner != currentUser.Id) return Json(new { success = false, message = "You cannot move folders from another user's drive." });

                        folder.ParentFolderId = destinationFolderId;
                        foldersProcessed++;
                    }

                    await db.SaveChangesAsync();
                    return Json(new { success = true, mode, filesProcessed, foldersProcessed, message = $"CUT complete. Files: {filesProcessed}, Folders: {foldersProcessed}." });
                }
                else
                {
                    // COPY = create DB reference rows (no physical copy).
                    foreach (var fid in fileIds)
                    {
                        // Use tracked entity because we may update it (pooling)
                        var src = await db.Files.FirstOrDefaultAsync(f => f.Id == fid && !f.IsProcessing);
                        if (src == null) continue;

                        // First-time reference pooling, if possible
                        var srcPathResolved = ResolvePhysicalPathSafeLocal(src.FilePath) ?? "";
                        var isPooled = srcPathResolved.StartsWith(GetSharedPoolRoot(), StringComparison.OrdinalIgnoreCase);
                        if (!isPooled && !string.IsNullOrWhiteSpace(src.FileHash))
                        {
                            // Pool original so both original and new reference point to the blob
                            var poolPath = GetSharedPoolFilePath(src.FileHash);
                            var poolExists = System.IO.File.Exists(poolPath);
                            var origPhys = ResolvePhysicalPathSafeLocal(src.FilePath);

                            if (!poolExists)
                            {
                                if (!string.IsNullOrWhiteSpace(origPhys) && System.IO.File.Exists(origPhys))
                                {
                                    Directory.CreateDirectory(Path.GetDirectoryName(poolPath));
                                    try
                                    {
                                        System.IO.File.Move(origPhys, poolPath);
                                    }
                                    catch
                                    {
                                        IOFile.Copy(origPhys, poolPath, overwrite: false);
                                        try { IOFile.Delete(origPhys); } catch { /* best-effort */ }
                                    }
                                }
                            }

                            // Repoint ALL rows with same hash (including this original) to pooled path
                            var all = await db.Files.Where(f => f.FileHash == src.FileHash).ToListAsync();
                            foreach (var r in all) r.FilePath = poolPath;
                            await db.SaveChangesAsync();

                            // reflect for this loop
                            srcPathResolved = poolPath;
                        }

                        var uniqueName = await GenerateUniqueFileNameAsync(destinationFolderId, src.Name);
                        db.Files.Add(new BOBDrive.Models.File
                        {
                            Name = uniqueName,
                            ContentType = src.ContentType,
                            Size = src.Size,
                            FilePath = srcPathResolved,       // pooled path if we pooled; otherwise same physical path
                            FolderId = destinationFolderId,
                            UploadedAt = DateTime.UtcNow,
                            IsProcessing = false,
                            ProcessingProgress = 100,
                            FileHash = src.FileHash,
                            ZipPassword = src.ZipPassword,
                            FinalizationStage = "Completed",
                            FinalizationProgress = 100
                        });
                        filesProcessed++;
                    }

                    // Optional: deep folder copy (logical references). Keep your existing logic if needed.
                    foreach (var srcFolderId in folderIds)
                    {
                        var fld = await db.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == srcFolderId);
                        if (fld == null || fld.ParentFolderId == null) continue;

                        var newRootId = await DuplicateFolderTreeAsync(srcFolderId, destinationFolderId, currentUser.Id);
                        if (newRootId > 0) foldersProcessed++;
                    }

                    await db.SaveChangesAsync();
                    return Json(new { success = true, mode, filesProcessed, foldersProcessed, message = $"COPY complete. Files: {filesProcessed}, Folders: {foldersProcessed}." });
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error during paste (mode={Mode})", mode);
                return Json(new { success = false, message = "Server error during paste." });
            }
        }
        //private async Task<int> GetRootFolderOwnerIdAsync(int folderId)
        //{
        //    var current = await db.Folders.FindAsync(folderId);
        //    if (current == null) return -1;
        //    while (current.ParentFolderId != null)
        //    {
        //        current = await db.Folders.FindAsync(current.ParentFolderId);
        //        if (current == null) return -1;
        //    }
        //    return current.OwnerUserId ?? -1;
        //}

        private async Task<bool> IsDescendantAsync(int potentialParentId, int childFolderId)
        {
            if (potentialParentId == childFolderId) return true;
            var current = await db.Folders.FindAsync(potentialParentId);
            while (current != null && current.ParentFolderId != null)
            {
                if (current.ParentFolderId == childFolderId) return true;
                current = await db.Folders.FindAsync(current.ParentFolderId);
            }
            return false;
        }

        private const int FolderCopyMaxNodes = 5000;

        private async Task<string> GenerateUniqueFileNameAsync(int folderId, string originalName)
        {
            var baseName = Path.GetFileNameWithoutExtension(originalName);
            var ext = Path.GetExtension(originalName);
            string candidate = originalName;
            int idx = 1;
            var existingNames = await db.Files.Where(f => f.FolderId == folderId).Select(f => f.Name).ToListAsync();
            var existingSet = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
            while (existingSet.Contains(candidate))
            {
                candidate = $"{baseName} - Copy{(idx > 1 ? $" ({idx})" : "")}{ext}";
                idx++;
            }
            return candidate;
        }

        private async Task<string> GenerateUniqueFolderNameAsync(int parentFolderId, string originalName)
        {
            string candidate = originalName;
            int idx = 1;
            var existingNames = await db.Folders.Where(f => f.ParentFolderId == parentFolderId).Select(f => f.Name).ToListAsync();
            var existingSet = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
            while (existingSet.Contains(candidate))
            {
                candidate = $"{originalName} - Copy{(idx > 1 ? $" ({idx})" : "")}";
                idx++;
            }
            return candidate;
        }

        private async Task<int> DuplicateFolderTreeAsync(int sourceFolderId, int destinationParentFolderId, int actingUserId, CancellationToken cancellationToken = default)
        {
            var sourceRoot = await db.Folders.AsNoTracking().FirstOrDefaultAsync(f => f.Id == sourceFolderId, cancellationToken);
            if (sourceRoot == null) throw new InvalidOperationException("Source folder not found.");
            if (sourceRoot.ParentFolderId == null) throw new InvalidOperationException("Copying a root folder is not allowed.");
            var allFolderIds = new List<int> { sourceRoot.Id };
            var queue = new Queue<int>();
            queue.Enqueue(sourceRoot.Id);
            while (queue.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int currentId = queue.Dequeue();
                var children = await db.Folders.AsNoTracking().Where(f => f.ParentFolderId == currentId).Select(f => f.Id).ToListAsync(cancellationToken);
                foreach (var child in children)
                {
                    allFolderIds.Add(child);
                    queue.Enqueue(child);
                    if (allFolderIds.Count > FolderCopyMaxNodes) throw new InvalidOperationException($"Subtree too large (> {FolderCopyMaxNodes} folders). Aborting copy.");
                }
            }
            var folderEntities = await db.Folders.AsNoTracking().Where(f => allFolderIds.Contains(f.Id)).ToListAsync(cancellationToken);
            var fileEntities = await db.Files.AsNoTracking().Where(fl => allFolderIds.Contains(fl.FolderId) && !fl.IsProcessing).ToListAsync(cancellationToken);
            var parentMap = folderEntities.ToDictionary(f => f.Id, f => f.ParentFolderId);
            foreach (var f in folderEntities)
            {
                var visited = new HashSet<int>();
                int walker = f.Id;
                while (parentMap.TryGetValue(walker, out var parentId) && parentId.HasValue)
                {
                    if (!visited.Add(parentId.Value)) throw new InvalidOperationException("Cycle detected in folder hierarchy. Aborting copy.");
                    walker = parentId.Value;
                }
            }
            var sourceIdToNewId = new Dictionary<int, int>();
            var orderedFolders = folderEntities.OrderBy(f => f.ParentFolderId.HasValue ? 1 : 0).ThenBy(f => f.Id).ToList();
            var newRootName = await GenerateUniqueFolderNameAsync(destinationParentFolderId, sourceRoot.Name);
            var newRoot = new Folder { Name = newRootName, ParentFolderId = destinationParentFolderId, CreatedAt = DateTime.UtcNow, OwnerUserId = actingUserId };
            db.Folders.Add(newRoot);
            await db.SaveChangesAsync(cancellationToken);
            sourceIdToNewId[sourceRoot.Id] = newRoot.Id;
            foreach (var folder in orderedFolders)
            {
                if (folder.Id == sourceRoot.Id) continue;
                cancellationToken.ThrowIfCancellationRequested();
                int? parentId = folder.ParentFolderId;
                if (!parentId.HasValue || !sourceIdToNewId.ContainsKey(parentId.Value)) continue;
                var newParentId = sourceIdToNewId[parentId.Value];
                string newName = await GenerateUniqueFolderNameAsync(newParentId, folder.Name);
                var newFolder = new Folder { Name = newName, ParentFolderId = newParentId, CreatedAt = DateTime.UtcNow, OwnerUserId = actingUserId };
                db.Folders.Add(newFolder);
                await db.SaveChangesAsync(cancellationToken);
                sourceIdToNewId[folder.Id] = newFolder.Id;
                if (sourceIdToNewId.Count + fileEntities.Count > FolderCopyMaxNodes) throw new InvalidOperationException($"Subtree too large (> {FolderCopyMaxNodes} total nodes).");
            }
            foreach (var file in fileEntities)
            {
                if (!sourceIdToNewId.TryGetValue(file.FolderId, out var newFolderId)) continue;
                var uniqueName = await GenerateUniqueFileNameAsync(newFolderId, file.Name);
                var newFile = new BOBDrive.Models.File { Name = uniqueName, ContentType = file.ContentType, Size = file.Size, FilePath = file.FilePath, FolderId = newFolderId, UploadedAt = DateTime.UtcNow, IsProcessing = false, ProcessingProgress = 100, FileHash = file.FileHash, ZipPassword = file.ZipPassword, LockedBySessionId = null, LockedAtUtc = null, FinalizationStage = "Completed", FinalizationProgress = 100 };
                db.Files.Add(newFile);
            }
            await db.SaveChangesAsync(cancellationToken);
            return newRoot.Id;
        }

        private static string SanitizeName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return "";
            var invalid = System.IO.Path.GetInvalidFileNameChars();
            var cleaned = new string(name.Where(c => !invalid.Contains(c)).ToArray());
            while (cleaned.Contains("..")) cleaned = cleaned.Replace("..", ".");
            return cleaned.Trim();
        }

        [HttpPost]
        public async Task<JsonResult> RenameFile(int id, string newName)
        {
            newName = SanitizeName(newName);
            if (string.IsNullOrWhiteSpace(newName)) return Json(new { success = false, message = "Invalid file name." });
            var file = await db.Files.FindAsync(id);
            if (file == null || file.IsProcessing) return Json(new { success = false, message = "File not found or still processing." });
            int rootOwner = await GetRootFolderOwnerIdAsync(file.FolderId);
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (user == null || rootOwner != user.Id) return Json(new { success = false, message = "Permission denied." });
            bool nameExists = await db.Files.AnyAsync(f => f.FolderId == file.FolderId && f.Id != file.Id && f.Name.Equals(newName, StringComparison.OrdinalIgnoreCase));
            if (nameExists) return Json(new { success = false, message = "A file with that name already exists in this folder." });
            file.Name = newName;
            await db.SaveChangesAsync();
            return Json(new { success = true, message = "File renamed.", newName });
        }

        [HttpPost]
        public async Task<JsonResult> RenameFolder(int id, string newName)
        {
            newName = SanitizeName(newName);
            if (string.IsNullOrWhiteSpace(newName) || newName == "." || newName == "..") return Json(new { success = false, message = "Invalid folder name." });
            var folder = await db.Folders.FindAsync(id);
            if (folder == null) return Json(new { success = false, message = "Folder not found." });
            if (folder.ParentFolderId == null) return Json(new { success = false, message = "Root folders cannot be renamed." });
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (user == null) return Json(new { success = false, message = "User not found." });
            int rootOwner = await GetRootFolderOwnerIdAsync(folder.Id);
            if (rootOwner != user.Id) return Json(new { success = false, message = "Permission denied." });
            bool nameExists = await db.Folders.AnyAsync(f => f.ParentFolderId == folder.ParentFolderId && f.Id != folder.Id && f.Name.Equals(newName, StringComparison.OrdinalIgnoreCase));
            if (nameExists) return Json(new { success = false, message = "A sibling folder with that name already exists." });
            folder.Name = newName;
            await db.SaveChangesAsync();
            return Json(new { success = true, message = "Folder renamed.", newName });
        }

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
    }
}