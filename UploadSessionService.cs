using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using tusdotnet.Interfaces;
using tusdotnet.Stores;
using System.IO;
using File = System.IO.File;

namespace BOBDrive.Services.Uploads
{
    // DB-first: Update the EDMX after running the SQL so CloudStorageDbContext has DbSet<UploadSession>.
    public class UploadSessionService
    {
        private readonly CloudStorageDbContext _db;
        private readonly ILogger _log;
        private readonly ILogger _auditLog;
        private readonly TusDiskStore _tusStore;
        private readonly string _tusBasePath;

        public UploadSessionService(CloudStorageDbContext db, ILogger log, ILogger auditLog, string tusStorePath, string tusBasePath = "/tus")
        {
            _db = db;
            _log = log;
            _auditLog = auditLog;
            _tusStore = new TusDiskStore(tusStorePath);
            _tusBasePath = tusBasePath;
        }

        public static string BuildUploadUrl(HttpRequest request, string tusBasePath, string tusFileId)
        {
            // app-relative path is sufficient for same-origin resume
            var basePath = VirtualPathUtility.ToAbsolute(tusBasePath.TrimEnd('/'));
            return basePath + "/" + HttpUtility.UrlPathEncode(tusFileId);
        }

        // REPLACE in UploadSessionService.cs
        public async Task<UploadSession> CreateOrAttachAsync(
            string tusFileId,
            string userExternalId,
            string filename,
            long? size,
            string sampleSha,
            string fullSha,
            string relativePath,
            int? targetFolderId)
        {
            var now = DateTime.UtcNow;
            var uploadUrl = BuildUploadUrl(HttpContext.Current?.Request, _tusBasePath, tusFileId);

            var owner = await _db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == userExternalId);
            var expiresAt = now.AddDays(UploadConfiguration.UploadSessionExpiryDays);

            // ✅ FIX: Retry loop with optimistic concurrency
            const int MAX_RETRIES = 3;
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++)
            {
                try
                {
                    // Use SQL transaction with UPDLOCK to prevent race
                    using (var transaction = _db.Database.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            // CRITICAL: Lock the row for UPDATE (prevents concurrent reads from seeing NULL)
                            var session = await _db.Database.SqlQuery<UploadSession>(
                                @"SELECT * FROM UploadSessions WITH (UPDLOCK, ROWLOCK) WHERE TusFileId = @p0",
                                tusFileId
                            ).FirstOrDefaultAsync();

                            if (session == null)
                            {
                                // Insert new session
                                session = new UploadSession
                                {
                                    TusFileId = tusFileId,
                                    UploadUrl = uploadUrl,
                                    Filename = filename,
                                    RelativePath = relativePath,
                                    SizeBytes = size ?? 0,
                                    OffsetBytes = 0,
                                    SampleSha256 = sampleSha,
                                    FullSha256 = fullSha,
                                    OwnerUserId = owner?.Id,
                                    TargetFolderId = targetFolderId,
                                    Status = 0,
                                    CreatedAt = now,
                                    UpdatedAt = now,
                                    ExpiresAt = expiresAt
                                };
                                _db.UploadSessions.Add(session);
                            }
                            else
                            {
                                // Update existing (idempotent resume)
                                session.UploadUrl = uploadUrl;
                                session.Filename = session.Filename ?? filename;
                                session.RelativePath = session.RelativePath ?? relativePath;
                                session.SizeBytes = size ?? session.SizeBytes;
                                session.SampleSha256 = session.SampleSha256 ?? sampleSha;
                                session.FullSha256 = session.FullSha256 ?? fullSha;
                                session.TargetFolderId = session.TargetFolderId ?? targetFolderId;
                                session.OwnerUserId = session.OwnerUserId ?? owner?.Id;
                                session.Status = 0;
                                session.UpdatedAt = now;
                                session.ExpiresAt = expiresAt;
                            }

                            await _db.SaveChangesAsync();
                            transaction.Commit();

                            return session;
                        }
                        catch
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex) when (attempt < MAX_RETRIES - 1)
                {
                    // Retry on deadlock/duplicate key (another thread won the race)
                    _log.Warning(ex, "CreateOrAttach retry {Attempt}/{Max} for {TusFileId}", attempt + 1, MAX_RETRIES, tusFileId);
                    await Task.Delay(50 * (attempt + 1)); // Exponential backoff: 50ms, 100ms, 150ms
                    continue;
                }
            }

            throw new InvalidOperationException($"Failed to create/attach session after {MAX_RETRIES} attempts");
        }


        public async Task<long> RefreshOffsetFromStoreAsync(string tusFileId)
        {
            long offset = 0;

            try
            {
                // Use the store interface to read current offset
                var store = (ITusStore)_tusStore;
                offset = await store.GetUploadOffsetAsync(tusFileId, default);
            }
            catch
            {
                // swallow; not critical for resume (tus handles offset)
            }

            var session = await _db.UploadSessions.FirstOrDefaultAsync(s => s.TusFileId == tusFileId);
            if (session != null)
            {
                session.OffsetBytes = offset;
                session.UpdatedAt = DateTime.UtcNow;
                await _db.SaveChangesAsync();
            }
            return offset;
        }

        public async Task MarkCompletedAsync(string tusFileId)
        {
            await RefreshOffsetFromStoreAsync(tusFileId);
            var s = await _db.UploadSessions.FirstOrDefaultAsync(x => x.TusFileId == tusFileId);
            if (s == null) return;
            s.Status = 1; // Completed
            s.UpdatedAt = DateTime.UtcNow;
            // Optional: extend retention of completed sessions if you want; here we keep same window.
            s.ExpiresAt = DateTime.UtcNow.AddDays(UploadConfiguration.UploadSessionExpiryDays);
            await _db.SaveChangesAsync();
        }

        public IQueryable<UploadSession> QueryIncompleteByChecksum(string sampleSha, string fullSha, long sizeBytes)
        {
            var q = _db.UploadSessions.Where(s =>
                s.Status == 0 &&
                s.SizeBytes == sizeBytes &&
                (
                    (!string.IsNullOrEmpty(fullSha) && s.FullSha256 == fullSha) ||
                    (!string.IsNullOrEmpty(sampleSha) && s.SampleSha256 == sampleSha)
                )
            );
            return q;
        }

        public async Task<bool> ClaimByTusIdAsync(string tusFileId, int claimerUserId, string claimerExternalId, int? targetFolderId)
        {
            var s = await _db.UploadSessions.FirstOrDefaultAsync(x => x.TusFileId == tusFileId);
            if (s == null || s.Status != 0) return false;

            // Allow re-claim at any time during the 15-day window
            s.OwnerUserId = claimerUserId;
            s.ClaimedByUserId = claimerUserId;
            s.ClaimedAt = DateTime.UtcNow;
            s.TargetFolderId = targetFolderId ?? s.TargetFolderId;
            s.UpdatedAt = DateTime.UtcNow;

            await _db.SaveChangesAsync();

            _auditLog?.Information("Upload session {TusFileId} claimed by {User} (UploadSessions.Id={Id})", s.TusFileId, claimerExternalId, s.Id);
            return true;
        }

        // Deep purge of all expired/incomplete sessions (called by the recurring job)
        public async Task<int> PurgeExpiredDeepAsync()
        {
            var now = DateTime.UtcNow;
            var victims = await _db.UploadSessions
                .Where(s => s.Status != 1 && s.ExpiresAt < now)
                .ToListAsync();

            int count = 0;
            foreach (var s in victims)
            {
                try
                {
                    await DeepDeleteForSessionAsync(s);
                    count++;
                }
                catch (Exception ex)
                {
                    _log.Error(ex, "Failed deep purge for session {TusFileId}", s.TusFileId);
                }
            }
            await _db.SaveChangesAsync();
            return count;
        }

        // One-off cleanup still available for targeted purge (not scheduled per-upload)
        public async Task<bool> PurgeOneIfExpiredDeepAsync(string tusFileId)
        {
            var s = await _db.UploadSessions.FirstOrDefaultAsync(x => x.TusFileId == tusFileId);
            if (s == null) return false;
            if (s.Status == 1) return false; // completed; don't purge as partial
            if (s.ExpiresAt > DateTime.UtcNow) return false;

            await DeepDeleteForSessionAsync(s);
            await _db.SaveChangesAsync();
            return true;
        }

        private async Task DeepDeleteForSessionAsync(UploadSession s)
        {
            // 1) Delete TUS physical file
            try { await _tusStore.DeleteFileAsync(s.TusFileId, default); } catch { }

            // 2) Delete legacy FileChunks rows (if any)
            try
            {
                var chunks = await _db.FileChunks.Where(c => c.FileIdForUpload == s.TusFileId).ToListAsync();
                if (chunks.Any())
                    _db.FileChunks.RemoveRange(chunks);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Failed to delete FileChunks for {TusFileId}", s.TusFileId);
            }

            // 3) Delete Files rows that reference this TUS and are not finalized
            try
            {
                var files = await _db.Files.Where(f => f.FileIdForUpload == s.TusFileId).ToListAsync();
                var toDelete = files.Where(f =>
                        f.IsProcessing == true ||
                        (f.FinalizationProgress.HasValue && f.FinalizationProgress.Value < 100) ||
                        string.IsNullOrWhiteSpace(f.FilePath) ||
                        (f.FilePath != null && !File.Exists(f.FilePath))
                    ).ToList();

                if (toDelete.Any())
                    _db.Files.RemoveRange(toDelete);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Failed to delete Files for {TusFileId}", s.TusFileId);
            }

            // 4) Finally, delete the UploadSession row
            try { _db.UploadSessions.Remove(s); } catch { }
        }
    }
}