using BOBDrive.Models;
using BOBDrive.Services;
using BOBDrive.Services.Uploads;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Hosting;
using System.Web.Mvc;
using File = BOBDrive.Models.File;

namespace BOBDrive.Controllers
{
    [Authorize]
    public class TusStatusController : Controller
    {
        [HttpGet]
        public async Task<ActionResult> Hash(string tusId)
        {
            if (string.IsNullOrWhiteSpace(tusId))
                return Json(new { success = false, message = "Missing tusId" }, JsonRequestBehavior.AllowGet);

            using (var db = new CloudStorageDbContext())
            {
                var rec = await db.Files
                    .OrderByDescending(f => f.Id)
                    .FirstOrDefaultAsync(f => f.FileIdForUpload == tusId);

                if (rec == null)
                    return Json(new { success = false, pending = true }, JsonRequestBehavior.AllowGet);

                var stage = rec.FinalizationStage ?? "";
                string hashMethod = stage.IndexOf("Sample", StringComparison.OrdinalIgnoreCase) >= 0 ? "sample" :
                                    stage.IndexOf("Full", StringComparison.OrdinalIgnoreCase) >= 0 ? "full" :
                                    "unknown";

                return Json(new
                {
                    success = true,
                    serverHash = rec.FileHash,
                    hashMethod = hashMethod,
                    size = rec.Size,
                    stage = rec.FinalizationStage,
                    progress = rec.FinalizationProgress
                }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        [ValidateInput(false)]
        public async Task<ActionResult> ExistsByHash()
        {
            string body;
            using (var reader = new StreamReader(Request.InputStream))
                body = await reader.ReadToEndAsync();

            HashExistsRequest payload = null;
            try { payload = JsonConvert.DeserializeObject<HashExistsRequest>(body); } catch { }

            var hashes = payload?.Hashes ?? new List<string>();
            if (hashes.Count == 0)
                return Json(new { success = true, existing = new string[0] });

            var norm = new HashSet<string>(hashes.Where(h => !string.IsNullOrWhiteSpace(h)).Select(h => h.ToLowerInvariant()));

            using (var db = new CloudStorageDbContext())
            {
                var existing = await db.Files.AsNoTracking()
                    .Where(f => f.FileHash != null && norm.Contains(f.FileHash.ToLower()) && f.IsProcessing == false)
                    .Select(f => f.FileHash)
                    .Distinct()
                    .ToListAsync();

                return Json(new { success = true, existing = existing });
            }
        }

        [HttpPost]
        [ValidateInput(false)]
        public async Task<ActionResult> CreateReference()
        {
            try
            {
                string body;
                using (var reader = new StreamReader(Request.InputStream))
                    body = await reader.ReadToEndAsync();

                var payload = JsonConvert.DeserializeObject<CreateReferenceRequest>(body ?? "{}") ?? new CreateReferenceRequest();

                if (string.IsNullOrWhiteSpace(payload.Hash))
                    return Json(new { success = false, message = "Missing hash" });

                var externalUserId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalUserId))
                    return Json(new { success = false, message = "Not authenticated" });

                using (var db = new CloudStorageDbContext())
                {
                    var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    if (user == null)
                        return Json(new { success = false, message = "User not found" });

                    var existing = await db.Files.AsNoTracking()
                        .Where(f => f.FileHash != null && f.FileHash.ToLower() == payload.Hash.ToLower() && f.IsProcessing == false)
                        .OrderByDescending(f => f.Id)
                        .FirstOrDefaultAsync();

                    if (existing == null || string.IsNullOrWhiteSpace(existing.FilePath) || !System.IO.File.Exists(existing.FilePath))
                        return Json(new { success = false, message = "No existing content for this hash" });

                    var rootFolder = await EnsureUserRootFolderAsync(db, user);
                    int destinationFolderId = rootFolder.Id;

                    if (payload.TargetFolderId.HasValue && await IsValidTargetFolderAsync(db, payload.TargetFolderId.Value, user.Id))
                    {
                        destinationFolderId = payload.TargetFolderId.Value;
                    }
                    else if (!string.IsNullOrWhiteSpace(payload.RelativePath))
                    {
                        var dirPart = GetDirectoryPart(payload.RelativePath);
                        if (!string.IsNullOrWhiteSpace(dirPart))
                        {
                            destinationFolderId = await EnsureFolderHierarchyAsync(db, user.Id, rootFolder.Id, dirPart);
                        }
                    }

                    var desiredName = !string.IsNullOrWhiteSpace(payload.Name)
                        ? payload.Name
                        : (!string.IsNullOrWhiteSpace(payload.RelativePath) ? System.IO.Path.GetFileName(payload.RelativePath) : existing.Name);

                    var finalName = await MakeUniqueDbNameAsync(db, destinationFolderId, desiredName);

                    var link = new File
                    {
                        Name = finalName,
                        ContentType = string.IsNullOrWhiteSpace(payload.ContentType) ? existing.ContentType : payload.ContentType,
                        Size = existing.Size,
                        FilePath = existing.FilePath,
                        FolderId = destinationFolderId,
                        UploadedAt = DateTime.UtcNow,
                        IsProcessing = false,
                        ProcessingProgress = 100,
                        FileHash = existing.FileHash,
                        FinalizationStage = "Linked (Reference Copy)",
                        FinalizationProgress = 100,
                        FileIdForUpload = null
                    };
                    db.Files.Add(link);
                    await db.SaveChangesAsync();

                    return Json(new { success = true, id = link.Id, name = link.Name, folderId = link.FolderId });
                }
            }
            catch (Exception ex)
            {
                Serilog.Log.Error(ex, "CreateReference failed");
                return Json(new { success = false, message = "Server error" });
            }
        }

        [HttpPost]
        [ValidateInput(false)]
        public async Task<ActionResult> QueryIncomplete()
        {
            try
            {
                string body;
                using (var r = new StreamReader(Request.InputStream))
                    body = await r.ReadToEndAsync();

                var payload = JsonConvert.DeserializeObject<IncompleteQueryRequest>(body ?? "{}") ?? new IncompleteQueryRequest();
                if (payload.Files == null || payload.Files.Count == 0)
                    return Json(new { success = true, results = new object[0] });

                var externalUserId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalUserId))
                    return Json(new { success = false, message = "Not authenticated" });

                var tusStorePath = HostingEnvironment.MapPath("~/App_Data/Tus/Store");

                using (var db = new CloudStorageDbContext())
                {
                    var me = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    var myId = me?.Id;

                    var svc = new UploadSessionService(db, Log.Logger, BOBDrive.App_Start.LoggingConfig.AuditLogger, tusStorePath, "/tus");
                    var results = new List<object>();

                    foreach (var f in payload.Files)
                    {
                        string keyHash = f.Hash ?? "";
                        long size = f.Size ?? 0;

                        var candidate = await svc
                            .QueryIncompleteByChecksum(sampleSha: keyHash, fullSha: keyHash, sizeBytes: size)
                            .OrderByDescending(s => s.UpdatedAt)
                            .FirstOrDefaultAsync();

                        if (candidate == null)
                        {
                            results.Add(new
                            {
                                hash = keyHash,
                                found = false,
                                fileIdForUpload = "",
                                uploadedBytes = 0L,
                                expectedBytes = size,
                                canResume = false,
                                ownedByCurrentUser = false,
                                differentUser = false
                            });
                            continue;
                        }

                        try { await svc.RefreshOffsetFromStoreAsync(candidate.TusFileId); } catch { }

                        bool mine = myId.HasValue && candidate.OwnerUserId == myId.Value;
                        bool canResume = candidate.Status == 0 && candidate.OffsetBytes < candidate.SizeBytes;

                        results.Add(new
                        {
                            hash = keyHash,
                            found = true,
                            fileIdForUpload = candidate.TusFileId,
                            uploadedBytes = candidate.OffsetBytes,
                            expectedBytes = candidate.SizeBytes,
                            canResume = canResume,
                            ownedByCurrentUser = mine,
                            differentUser = !mine,
                            uploadUrl = candidate.UploadUrl
                        });
                    }

                    return Json(new { success = true, results = results });
                }
            }
            catch (Exception ex)
            {
                Serilog.Log.Error(ex, "QueryIncomplete failed");
                return Json(new { success = false, message = "Server error" });
            }
        }

        [HttpPost]
        [ValidateInput(false)]
        public async Task<ActionResult> ClaimIncomplete()
        {
            try
            {
                string body;
                using (var r = new StreamReader(Request.InputStream))
                    body = await r.ReadToEndAsync();

                var payload = JsonConvert.DeserializeObject<ClaimIncompleteRequest>(body ?? "{}") ?? new ClaimIncompleteRequest();
                if (string.IsNullOrWhiteSpace(payload.FileIdForUpload))
                    return Json(new { success = false, message = "Missing fileIdForUpload" });

                var externalUserId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalUserId))
                    return Json(new { success = false, message = "Not authenticated" });

                var tusStorePath = HostingEnvironment.MapPath("~/App_Data/Tus/Store");

                using (var db = new CloudStorageDbContext())
                {
                    var me = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    if (me == null)
                        return Json(new { success = false, message = "User not found" });

                    var svc = new UploadSessionService(db, Log.Logger, BOBDrive.App_Start.LoggingConfig.AuditLogger, tusStorePath, "/tus");
                    var ok = await svc.ClaimByTusIdAsync(payload.FileIdForUpload, me.Id, externalUserId, payload.TargetFolderId);
                    if (!ok) return Json(new { success = false, message = "Cannot claim (not found or completed)" });

                    return Json(new { success = true });
                }
            }
            catch (Exception ex)
            {
                Serilog.Log.Error(ex, "ClaimIncomplete failed");
                return Json(new { success = false, message = "Server error" });
            }
        }

        // Logs user + IP for every decision the client makes
        [HttpPost]
        [ValidateInput(false)]
        public async Task<ActionResult> LogDecision()
        {
            try
            {
                string body;
                using (var reader = new StreamReader(Request.InputStream))
                    body = await reader.ReadToEndAsync();

                var payload = JsonConvert.DeserializeObject<ClientDecisionLogRequest>(body ?? "{}") ?? new ClientDecisionLogRequest();

                var user = User?.Identity?.Name ?? "(anonymous)";
                var ip = GetClientIp();

                // TUS session starts / resumes
                var decision = (payload.Decision ?? "").ToLowerInvariant();
                if (decision == "fresh" || decision == "resume" || decision == "upload_anyway")
                {
                    ClientActivityMonitor.OnTusSessionStarted(ip);
                }

                Log.Information("Upload decision by {User} from {IP}. Decision={Decision}, Name={Name}, Size={Size}, Hash={Hash}, RelativePath={RelativePath}, ResumableFound={ResumableFound}, DuplicateFound={DuplicateFound}, ResumeTusId={ResumeTusId}, UploadedBytes={UploadedBytes}, ExpectedBytes={ExpectedBytes}, ClientGuid={ClientUploadGuid}",
                    user, ip,
                    payload.Decision, payload.Name, payload.Size, payload.Hash, payload.RelativePath,
                    payload.ResumableFound, payload.DuplicateFound, payload.ResumeTusId, payload.UploadedBytes, payload.ExpectedBytes,
                    payload.ClientUploadGuid
                );

                return Json(new { success = true });
            }
            catch (Exception ex)
            {
                Log.Error(ex, "LogDecision failed");
                return Json(new { success = false, message = "Server error" });
            }
        }



        private string GetClientIp()
        {
            try
            {
                var xff = Request.ServerVariables["HTTP_X_FORWARDED_FOR"];
                if (!string.IsNullOrWhiteSpace(xff)) return xff.Split(',').First().Trim();
                return Request.UserHostAddress;
            }
            catch { return "(unknown)"; }
        }

        public class HashExistsRequest { public List<string> Hashes { get; set; } }
        public class CreateReferenceRequest
        {
            public string Hash { get; set; }
            public string HashMethod { get; set; }
            public string SampleSpec { get; set; }
            public string Name { get; set; }
            public string ContentType { get; set; }
            public long? Size { get; set; }
            public string RelativePath { get; set; }
            public int? TargetFolderId { get; set; }
        }
        public class IncompleteQueryRequest { public List<IncompleteQueryItem> Files { get; set; } }
        public class IncompleteQueryItem { public string Hash { get; set; } public long? Size { get; set; } public string Name { get; set; } }
        public class ClaimIncompleteRequest { public string FileIdForUpload { get; set; } public int? TargetFolderId { get; set; } }
        public class ClientDecisionLogRequest
        {
            public string Decision { get; set; } // skip | duplicate | resume | upload_anyway | fresh
            public string Hash { get; set; }
            public long? Size { get; set; }
            public string Name { get; set; }
            public string RelativePath { get; set; }
            public bool? ResumableFound { get; set; }
            public bool? DuplicateFound { get; set; }
            public string ResumeTusId { get; set; }
            public long? UploadedBytes { get; set; }
            public long? ExpectedBytes { get; set; }
            public string ClientUploadGuid { get; set; }
        }

        private static string GetDirectoryPart(string path) =>
            System.IO.Path.GetDirectoryName(path == null ? null : path.Replace('\\', '/'))?.Replace('\\', '/');

        private async Task<Folder> EnsureUserRootFolderAsync(CloudStorageDbContext db, User user)
        {
            var root = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id);
            if (root == null)
            {
                root = new Folder { Name = user.ExternalUserId, ParentFolderId = null, OwnerUserId = user.Id, CreatedAt = DateTime.UtcNow };
                db.Folders.Add(root);
                await db.SaveChangesAsync();
            }
            return root;
        }

        private async Task<bool> IsValidTargetFolderAsync(CloudStorageDbContext db, int folderId, int userId)
        {
            var candidate = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId);
            if (candidate == null) return false;
            var ownerId = await GetRootOwnerIdAsync(db, candidate.Id);
            return ownerId == userId;
        }

        private static async Task<int> GetRootOwnerIdAsync(CloudStorageDbContext db, int folderId)
        {
            var current = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId);
            if (current == null) return -1;
            while (current.ParentFolderId != null)
            {
                current = await db.Folders.FirstOrDefaultAsync(f => f.Id == current.ParentFolderId.Value);
                if (current == null) return -1;
            }
            return current.OwnerUserId ?? -1;
        }

        private static async Task<int> EnsureFolderHierarchyAsync(CloudStorageDbContext db, int ownerUserId, int baseFolderId, string relativeFolderPath)
        {
            var segments = (relativeFolderPath ?? string.Empty).Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            int currentParentId = baseFolderId;
            foreach (var segment in segments)
            {
                var existing = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == currentParentId && f.Name.Equals(segment, StringComparison.OrdinalIgnoreCase));
                if (existing != null) currentParentId = existing.Id;
                else
                {
                    var newFolder = new Folder { Name = segment, ParentFolderId = currentParentId, OwnerUserId = ownerUserId, CreatedAt = DateTime.UtcNow };
                    db.Folders.Add(newFolder);
                    await db.SaveChangesAsync();
                    currentParentId = newFolder.Id;
                }
            }
            return currentParentId;
        }


        private static async Task<string> MakeUniqueDbNameAsync(CloudStorageDbContext db, int folderId, string desiredName)
        {
            var stem = System.IO.Path.GetFileNameWithoutExtension(desiredName);
            var ext = System.IO.Path.GetExtension(desiredName) ?? "";
            var candidate = desiredName;
            int i = 1;
            for (; ; )
            {
                var exists = await db.Files.AnyAsync(f => f.FolderId == folderId && f.Name == candidate);
                if (!exists) return candidate;
                candidate = $"{stem}({i++}){ext}";
            }
        }
    }
}