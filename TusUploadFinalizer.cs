using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Security;
using tusdotnet.Interfaces;
using tusdotnet.Models;
using tusdotnet.Models.Configuration;
using File = System.IO.File;
using FileModel = BOBDrive.Models.File;

namespace BOBDrive.Services.Tus
{
    public class TusUploadFinalizer
    {
        private readonly ILogger _log = Log.ForContext<TusUploadFinalizer>();
        private const long LARGE_SAMPLE_ONLY_THRESHOLD = 1L * 1024 * 1024 * 1024;

        // Windows path constraints
        private const int MAX_PATH = 260;
        private const int MAX_DIR = 248;

        private class HashCopyResult
        {
            public string FullHash;
            public string SampleHash;
            public long BytesWritten;
        }

        /// <summary>
        /// PRODUCTION-READY TUS FINALIZATION WITH LOCK MANAGEMENT
        /// </summary>
        public async Task HandleAsync(FileCompleteContext ctx)
        {
            var logger = _log.ForContext("Area", "TUS-Finalize")
                             .ForContext("TusFileId", ctx.FileId)
                             .ForContext("User", GetExternalUserId(ctx))
                             .ForContext("Timestamp", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"));

            string tempFilePath = null;
            bool shouldTerminate = false;
            CancellationTokenSource lockRefreshCts = null;
            Task lockRefreshTask = null;

            // NEW: try to determine client IP (best-effort)
            string clientIp = "(unknown)";
            try
            {
                var http = System.Web.HttpContext.Current;
                if (http != null)
                {
                    var xff = http.Request.ServerVariables["HTTP_X_FORWARDED_FOR"];
                    if (!string.IsNullOrWhiteSpace(xff))
                        clientIp = xff.Split(',').First().Trim();
                    else
                        clientIp = http.Request.UserHostAddress;
                }
            }
            catch
            {
                // ignore – stats are best-effort
            }

            try
            {
                var readable = ctx.Store as ITusReadableStore;
                if (readable == null)
                {
                    logger.Error("Store does not implement ITusReadableStore.");
                    return;
                }

                var tusFile = await readable.GetFileAsync(ctx.FileId, ctx.CancellationToken).ConfigureAwait(false);
                if (tusFile == null)
                {
                    logger.Error("TUS file not found in store.");
                    return;
                }

                var meta = await GetNormalizedMetadataAsync(tusFile, ctx.CancellationToken).ConfigureAwait(false);
                Func<string, string> GetMeta = k =>
                {
                    string v;
                    return meta.TryGetValue(k, out v) ? v : null;
                };

                long? expectedBytes = null;
                if (long.TryParse(GetMeta("size"), out var szParsed) && szParsed > 0)
                    expectedBytes = szParsed;

                bool isLargeSampleOnly = expectedBytes.HasValue && expectedBytes.Value > LARGE_SAMPLE_ONLY_THRESHOLD;

                var externalUserId = GetExternalUserId(ctx);
                if (string.IsNullOrWhiteSpace(externalUserId))
                {
                    logger.Error("User not found (ExternalUserId=null). Aborting finalization.");
                    return;
                }

                string filenameFromMeta = GetMeta("filename");
                string relativePath = GetMeta("relativePath") ?? GetMeta("relative_path") ?? GetMeta("path");

                string originalName = null;
                if (!string.IsNullOrWhiteSpace(filenameFromMeta) &&
                    !filenameFromMeta.Equals("null", StringComparison.OrdinalIgnoreCase))
                {
                    originalName = filenameFromMeta;
                }
                else if (!string.IsNullOrWhiteSpace(relativePath))
                {
                    originalName = Path.GetFileName(relativePath);
                }
                if (string.IsNullOrWhiteSpace(originalName))
                {
                    originalName = ctx.FileId + ".bin";
                    logger.Warning("Filename not found; using fallback: {Name}", originalName);
                }

                string contentType = GetMeta("filetype") ?? "application/octet-stream";

                string clientFullHash = SafeTrim(GetMeta("sha256"));
                string clientSampleHash = SafeTrim(GetMeta("sampleSha256"));
                string clientSampleSpec = SafeTrim(GetMeta("sampleSpec"));
                ParseSampleSpec(clientSampleSpec, out var sampleCount, out var samplePct);
                if (sampleCount <= 0) sampleCount = 8;
                if (samplePct <= 0) samplePct = 0.02;

                if (isLargeSampleOnly && string.IsNullOrWhiteSpace(clientSampleHash))
                {
                    logger.Error("Large file (>1 GiB) requires client sample hash. Rejecting.");
                    await TryTerminateTusAsync(ctx).ConfigureAwait(false);
                    return;
                }

                var tempCompletedDir = Path.Combine(UploadConfiguration.TempMergePath, "TusCompleted");
                Directory.CreateDirectory(tempCompletedDir);
                tempFilePath = Path.Combine(tempCompletedDir, ctx.FileId + "-" + Path.GetFileName(originalName));

                logger.Information("Starting finalization: {FileName} ({Size} bytes, Mode: {Mode})",
                    originalName, expectedBytes ?? 0, isLargeSampleOnly ? "sample-only" : "full");

                // ========== START LOCK REFRESH BACKGROUND TASK ==========
                lockRefreshCts = new CancellationTokenSource();
                lockRefreshTask = StartLockRefreshAsync(ctx.FileId, lockRefreshCts.Token, logger);
                logger.Information("Lock refresh task started for {FileId}", ctx.FileId);

                // ========== STREAM AND HASH FILE ==========
                HashCopyResult copyResult;
                int lastPctLogged = -1;

                if (isLargeSampleOnly)
                {
                    logger.Information("Processing as LARGE file (sample-only verification)");
                    copyResult = await StreamToTempAndHashSampleOnlyAsync(
                        tusFile,
                        tempFilePath,
                        ctx.CancellationToken,
                        expectedBytes,
                        sampleCount,
                        samplePct,
                        (done, total) =>
                        {
                            if (total.HasValue && total.Value > 0)
                            {
                                var pct = (int)Math.Floor(done * 100.0 / total.Value);
                                if (pct != lastPctLogged && (pct % 5 == 0 || pct == 100))
                                {
                                    lastPctLogged = pct;
                                    logger.Information("Server hashing (sample-only): {Pct}% ({Done}/{Total} bytes)", pct, done, total);
                                }
                            }
                        }).ConfigureAwait(false);
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(clientSampleHash))
                    {
                        logger.Information("Processing with FULL + SAMPLE verification");
                        copyResult = await StreamToTempAndHashWithSampleAsync(
                            tusFile,
                            tempFilePath,
                            ctx.CancellationToken,
                            expectedBytes,
                            sampleCount,
                            samplePct,
                            (done, total) =>
                            {
                                if (total.HasValue && total.Value > 0)
                                {
                                    var pct = (int)Math.Floor(done * 100.0 / total.Value);
                                    if (pct != lastPctLogged && (pct % 5 == 0 || pct == 100))
                                    {
                                        lastPctLogged = pct;
                                        logger.Information("Server hashing (full+sample): {Pct}% ({Done}/{Total} bytes)", pct, done, total);
                                    }
                                }
                            }).ConfigureAwait(false);
                    }
                    else
                    {
                        logger.Information("Processing with FULL verification only");
                        copyResult = await StreamToTempAndHashFullOnlyAsync(
                            tusFile,
                            tempFilePath,
                            ctx.CancellationToken,
                            expectedBytes,
                            (done, total) =>
                            {
                                if (total.HasValue && total.Value > 0)
                                {
                                    var pct = (int)Math.Floor(done * 100.0 / total.Value);
                                    if (pct != lastPctLogged && (pct % 5 == 0 || pct == 100))
                                    {
                                        lastPctLogged = pct;
                                        logger.Information("Server hashing (full): {Pct}% ({Done}/{Total} bytes)", pct, done, total);
                                    }
                                }
                            }).ConfigureAwait(false);
                    }
                }

                if (copyResult == null || (isLargeSampleOnly
                        ? string.IsNullOrWhiteSpace(copyResult.SampleHash)
                        : (string.IsNullOrWhiteSpace(copyResult.FullHash) && string.IsNullOrWhiteSpace(copyResult.SampleHash))))
                {
                    logger.Error("Failed to compute required checksum while copying.");
                    TryDeleteFile(tempFilePath);
                    return;
                }

                long fileSize = copyResult.BytesWritten;

                // session done
                ClientActivityMonitor.OnTusSessionCompleted(clientIp);

                // show a short-lived "processing" weight based on bytes
                ClientActivityMonitor.OnFinalizationBytesStarted(clientIp, fileSize);

                string serverStoredHash;
                string finalStageNote;

                // ========== VERIFY CHECKSUMS ==========
                if (isLargeSampleOnly)
                {
                    if (!clientSampleHash.Equals(copyResult.SampleHash, StringComparison.OrdinalIgnoreCase))
                    {
                        logger.Error("❌ SAMPLE HASH MISMATCH (large). Client={Client}, Server={Server}",
                            clientSampleHash, copyResult.SampleHash);
                        TryDeleteFile(tempFilePath);
                        await TryTerminateTusAsync(ctx).ConfigureAwait(false);
                        return;
                    }
                    logger.Information("✅ SAMPLE HASH MATCH (large file verified)");
                    serverStoredHash = copyResult.SampleHash;
                    finalStageNote = "Completed (Sample Verified)";
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(clientFullHash) && !string.IsNullOrWhiteSpace(copyResult.FullHash))
                    {
                        if (!clientFullHash.Equals(copyResult.FullHash, StringComparison.OrdinalIgnoreCase))
                        {
                            logger.Error("❌ FULL HASH MISMATCH. Client={Client}, Server={Server}",
                                clientFullHash, copyResult.FullHash);
                            TryDeleteFile(tempFilePath);
                            await TryTerminateTusAsync(ctx).ConfigureAwait(false);
                            return;
                        }
                        logger.Information("✅ FULL HASH MATCH (integrity verified)");
                    }
                    else if (!string.IsNullOrWhiteSpace(clientSampleHash) && !string.IsNullOrWhiteSpace(copyResult.SampleHash))
                    {
                        if (!clientSampleHash.Equals(copyResult.SampleHash, StringComparison.OrdinalIgnoreCase))
                        {
                            logger.Error("❌ SAMPLE HASH MISMATCH. Client={Client}, Server={Server}",
                                clientSampleHash, copyResult.SampleHash);
                            TryDeleteFile(tempFilePath);
                            await TryTerminateTusAsync(ctx).ConfigureAwait(false);
                            return;
                        }
                        logger.Information("✅ SAMPLE HASH MATCH + Full hash stored");
                    }
                    else
                    {
                        logger.Warning("⚠️ No client checksum provided; accepting based on server hash only.");
                    }

                    serverStoredHash = copyResult.FullHash ?? copyResult.SampleHash;
                    finalStageNote = !string.IsNullOrWhiteSpace(clientFullHash) ? "Completed (Full Verified)" :
                                     !string.IsNullOrWhiteSpace(clientSampleHash) ? "Completed (Sample Verified + Full Stored)" :
                                     "Completed (Server Full Only)";
                }

                logger.Information("Checksum verification complete. Stored hash: {Hash}", serverStoredHash);

                // ========== MOVE TO FINAL LOCATION & UPDATE DB ==========
                using (var db = new CloudStorageDbContext())
                {
                    var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId, ctx.CancellationToken).ConfigureAwait(false);
                    if (user == null)
                    {
                        logger.Error("No DB user for {User}. Keeping temp file: {File}", externalUserId, tempFilePath);
                        return;
                    }

                    var userPhysicalRoot = Path.Combine(UploadConfiguration.FinalUploadPath, user.ExternalUserId);
                    ExtendedCreateDirectory(userPhysicalRoot);

                    var rootFolder = await EnsureUserRootFolderAsync(db, user, ctx.CancellationToken).ConfigureAwait(false);

                    int destinationFolderId = rootFolder.Id;
                    string destinationPhysicalPath = userPhysicalRoot;

                    int? targetFolderId = TryParseInt(GetMeta("targetFolderId") ?? GetMeta("folderId"));
                    if (targetFolderId.HasValue &&
                        await IsValidTargetFolderAsync(db, targetFolderId.Value, user.Id, ctx.CancellationToken).ConfigureAwait(false))
                    {
                        destinationFolderId = targetFolderId.Value;
                    }

                    if (!string.IsNullOrWhiteSpace(relativePath))
                    {
                        var directoryPart = NormalizeRelativeDir(relativePath);
                        if (!string.IsNullOrWhiteSpace(directoryPart))
                        {
                            destinationFolderId = await EnsureFolderHierarchyAsync(
                                db,
                                user.Id,
                                rootFolder.Id,
                                directoryPart,
                                ctx.CancellationToken).ConfigureAwait(false);

                            destinationPhysicalPath = Path.Combine(
                                userPhysicalRoot,
                                directoryPart.Replace('/', Path.DirectorySeparatorChar));

                            ExtendedCreateDirectory(destinationPhysicalPath);
                        }
                    }

                    var existingRecord = await db.Files
                        .FirstOrDefaultAsync(f => f.FileIdForUpload == ctx.FileId, ctx.CancellationToken)
                        .ConfigureAwait(false);

                    string finalName;
                    if (existingRecord != null &&
                        existingRecord.FolderId == destinationFolderId &&
                        !string.IsNullOrWhiteSpace(existingRecord.Name))
                    {
                        finalName = existingRecord.Name;
                        logger.Information("Reusing existing file name on resume: {Name}", finalName);
                    }
                    else
                    {
                        finalName = MakeUniqueName(destinationPhysicalPath, originalName);
                    }

                    var finalPath = Path.Combine(destinationPhysicalPath, finalName);

                    EnsureDirectoryForFile(finalPath);

                    if (ExtendedFileExists(finalPath))
                    {
                        try { ExtendedFileDelete(finalPath); } catch { }
                    }

                    logger.Information("Moving file: {Source} → {Dest}", tempFilePath, finalPath);

                    for (int attempt = 0; attempt < 2; attempt++)
                    {
                        try
                        {
                            ExtendedFileMove(tempFilePath, finalPath);
                            break;
                        }
                        catch (DirectoryNotFoundException ex) when (attempt == 0)
                        {
                            logger.Warning(ex, "Destination directory missing at move time. Recreating and retrying.");
                            EnsureDirectoryForFile(finalPath);
                            continue;
                        }
                    }

                    logger.Information("File successfully moved to: {FinalPath}", finalPath);

                    // UPDATE DATABASE
                    if (existingRecord != null)
                    {
                        existingRecord.Name = finalName;
                        existingRecord.ContentType = contentType;
                        existingRecord.Size = fileSize;
                        existingRecord.FilePath = finalPath;
                        existingRecord.FolderId = destinationFolderId;
                        existingRecord.IsProcessing = false;
                        existingRecord.ProcessingProgress = 100;
                        existingRecord.FileHash = serverStoredHash;
                        existingRecord.FinalizationStage = finalStageNote;
                        existingRecord.FinalizationProgress = 100;
                        existingRecord.LockedBySessionId = null;
                        existingRecord.LockedAtUtc = null;
                        await db.SaveChangesAsync(ctx.CancellationToken).ConfigureAwait(false);
                        logger.Information("✅ DB record updated (Id={Id}).", existingRecord.Id);
                    }
                    else
                    {
                        var rec = new FileModel
                        {
                            Name = finalName,
                            ContentType = contentType,
                            Size = fileSize,
                            FilePath = finalPath,
                            FolderId = destinationFolderId,
                            UploadedAt = DateTime.UtcNow,
                            IsProcessing = false,
                            ProcessingProgress = 100,
                            FileHash = serverStoredHash,
                            FinalizationStage = finalStageNote,
                            FinalizationProgress = 100,
                            FileIdForUpload = ctx.FileId
                        };
                        db.Files.Add(rec);
                        await db.SaveChangesAsync(ctx.CancellationToken).ConfigureAwait(false);
                        logger.Information("✅ DB record inserted (Id={Id}).", rec.Id);
                    }
                }
                ClientActivityMonitor.OnFinalizationBytesCompleted(clientIp, fileSize);
                shouldTerminate = true;
                logger.Information("🎉 Finalization completed successfully for {FileName}", originalName);
            }
            catch (OperationCanceledException)
            {
                logger.Warning("⚠️ Finalization canceled for {FileId}", ctx.FileId);
                shouldTerminate = false;
            }
            catch (IOException ioex) when (ioex.Message.IndexOf("being used by another process", StringComparison.OrdinalIgnoreCase) >= 0
                                        || ioex.Message.IndexOf("pipe has been ended", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                logger.Warning(ioex, "⚠️ Transient IO error; leaving upload intact for retry.");
                shouldTerminate = false;
                throw;
            }
            catch (Exception ex)
            {
                logger.Error(ex, "❌ TUS finalization failed unexpectedly.");
                shouldTerminate = false;
                if (tempFilePath != null)
                {
                    logger.Warning("Keeping temporary file for inspection: {TempFilePath}", tempFilePath);
                }
            }
            finally
            {
                // STOP LOCK REFRESH
                if (lockRefreshCts != null)
                {
                    lockRefreshCts.Cancel();
                    if (lockRefreshTask != null)
                    {
                        try { await lockRefreshTask.ConfigureAwait(false); }
                        catch (OperationCanceledException) { }
                        catch (Exception ex) { logger.Warning(ex, "Lock refresh task threw exception on cleanup"); }
                    }
                    lockRefreshCts.Dispose();
                    logger.Information("Lock refresh task stopped for {FileId}", ctx.FileId);
                }

                // CLEANUP TUS RECORD
                if (shouldTerminate)
                {
                    await TryTerminateTusAsync(ctx).ConfigureAwait(false);
                    logger.Information("TUS record terminated for {FileId}", ctx.FileId);
                }
                else
                {
                    logger.Information("TUS record preserved for potential retry: {FileId}", ctx.FileId);
                }
            }
        }

        /// <summary>
        /// Background task that refreshes the database lock timestamp every 30 seconds
        /// to prevent timeout during long finalization operations.
        /// </summary>
        private async Task StartLockRefreshAsync(string tusFileId, CancellationToken cancellationToken, ILogger logger)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(30000, cancellationToken); // 30 seconds

                    using (var db = new CloudStorageDbContext())
                    {
                        var rec = await db.Files.FirstOrDefaultAsync(f => f.FileIdForUpload == tusFileId, cancellationToken);
                        if (rec != null && rec.IsProcessing)
                        {
                            rec.LockedAtUtc = DateTime.UtcNow;
                            await db.SaveChangesAsync(cancellationToken);
                            logger.Debug("🔄 Lock refreshed for {FileId} at {Time}", tusFileId, DateTime.UtcNow.ToString("HH:mm:ss"));
                        }
                        else
                        {
                            logger.Debug("Lock refresh skipped (record not processing or not found): {FileId}", tusFileId);
                            break;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when finalization completes
                logger.Debug("Lock refresh canceled normally for {FileId}", tusFileId);
            }
            catch (Exception ex)
            {
                logger.Warning(ex, "Lock refresh failed for {FileId}", tusFileId);
            }
        }

        #region Hash & Streaming Helpers
        private static async Task<HashCopyResult> StreamToTempAndHashFullOnlyAsync(
            ITusFile tusFile,
            string tempFilePath,
            CancellationToken ct,
            long? expectedBytes,
            Action<long, long?> progress)
        {
            const int BufferSize = 4 * 1024 * 1024;
            Directory.CreateDirectory(Path.GetDirectoryName(tempFilePath) ?? ".");
            using (Stream src = await tusFile.GetContentAsync(ct).ConfigureAwait(false))
            using (FileStream dst = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan))
            using (SHA256 shaFull = SHA256.Create())
            {
                byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
                long bytesWritten = 0;
                try
                {
                    for (; ; )
                    {
                        int read = await src.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                        if (read <= 0) break;
                        await dst.WriteAsync(buffer, 0, read, ct).ConfigureAwait(false);
                        shaFull.TransformBlock(buffer, 0, read, null, 0);
                        bytesWritten += read;
                        progress?.Invoke(bytesWritten, expectedBytes);
                    }
                    shaFull.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                    return new HashCopyResult { FullHash = BytesToHex(shaFull.Hash), SampleHash = null, BytesWritten = bytesWritten };
                }
                finally { ArrayPool<byte>.Shared.Return(buffer); }
            }
        }

        private static async Task<HashCopyResult> StreamToTempAndHashWithSampleAsync(
            ITusFile tusFile,
            string tempFilePath,
            CancellationToken ct,
            long? expectedBytes,
            int sampleCount,
            double samplePct,
            Action<long, long?> progress)
        {
            const int BufferSize = 4 * 1024 * 1024;
            Directory.CreateDirectory(Path.GetDirectoryName(tempFilePath) ?? ".");
            using (Stream src = await tusFile.GetContentAsync(ct).ConfigureAwait(false))
            using (FileStream dst = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan))
            using (SHA256 shaFull = SHA256.Create())
            using (SHA256 shaSample = SHA256.Create())
            {
                long bytesWritten = 0;
                long total = expectedBytes.HasValue && expectedBytes.Value > 0 ? expectedBytes.Value : long.MaxValue;
                var windows = ComputeSampleWindows(total, sampleCount, samplePct);
                int w = 0;

                byte[] header = Encoding.UTF8.GetBytes("size:" + total + ";samples:" + sampleCount + ";pct:" + samplePct.ToString(System.Globalization.CultureInfo.InvariantCulture));
                shaSample.TransformBlock(header, 0, header.Length, null, 0);

                byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
                try
                {
                    for (; ; )
                    {
                        int read = await src.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                        if (read <= 0) break;

                        await dst.WriteAsync(buffer, 0, read, ct).ConfigureAwait(false);
                        shaFull.TransformBlock(buffer, 0, read, null, 0);

                        if (w < windows.Count)
                        {
                            long chunkStart = bytesWritten;
                            long chunkEnd = bytesWritten + read;
                            while (w < windows.Count)
                            {
                                var ws = windows[w].Item1;
                                var we = windows[w].Item2;
                                if (we <= chunkStart) { w++; continue; }
                                if (ws >= chunkEnd) break;
                                long s = Math.Max(chunkStart, ws);
                                long e = Math.Min(chunkEnd, we);
                                int offset = (int)(s - chunkStart);
                                int count = (int)(e - s);
                                if (count > 0) shaSample.TransformBlock(buffer, offset, count, null, 0);
                                if (we <= chunkEnd) w++; else break;
                            }
                        }

                        bytesWritten += read;
                        progress?.Invoke(bytesWritten, expectedBytes);
                    }

                    shaFull.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                    shaSample.TransformFinalBlock(Array.Empty<byte>(), 0, 0);

                    return new HashCopyResult
                    {
                        FullHash = BytesToHex(shaFull.Hash),
                        SampleHash = BytesToHex(shaSample.Hash),
                        BytesWritten = bytesWritten
                    };
                }
                finally { ArrayPool<byte>.Shared.Return(buffer); }
            }
        }

        private static async Task<HashCopyResult> StreamToTempAndHashSampleOnlyAsync(
            ITusFile tusFile,
            string tempFilePath,
            CancellationToken ct,
            long? expectedBytes,
            int sampleCount,
            double samplePct,
            Action<long, long?> progress)
        {
            const int BufferSize = 4 * 1024 * 1024;
            Directory.CreateDirectory(Path.GetDirectoryName(tempFilePath) ?? ".");
            using (Stream src = await tusFile.GetContentAsync(ct).ConfigureAwait(false))
            using (FileStream dst = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan))
            using (SHA256 shaSample = SHA256.Create())
            {
                long bytesWritten = 0;

                if (!expectedBytes.HasValue || expectedBytes.Value <= 0)
                {
                    byte[] b = ArrayPool<byte>.Shared.Rent(BufferSize);
                    try
                    {
                        for (; ; )
                        {
                            int r = await src.ReadAsync(b, 0, b.Length, ct).ConfigureAwait(false);
                            if (r <= 0) break;
                            await dst.WriteAsync(b, 0, r, ct).ConfigureAwait(false);
                            bytesWritten += r;
                            progress?.Invoke(bytesWritten, null);
                        }
                    }
                    finally { ArrayPool<byte>.Shared.Return(b); }
                    return new HashCopyResult { BytesWritten = bytesWritten };
                }

                long total = expectedBytes.Value;
                var windows = ComputeSampleWindows(total, sampleCount, samplePct);
                int w = 0;

                byte[] header = Encoding.UTF8.GetBytes("size:" + total + ";samples:" + sampleCount + ";pct:" + samplePct.ToString(System.Globalization.CultureInfo.InvariantCulture));
                shaSample.TransformBlock(header, 0, header.Length, null, 0);

                byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
                try
                {
                    for (; ; )
                    {
                        int read = await src.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                        if (read <= 0) break;

                        await dst.WriteAsync(buffer, 0, read, ct).ConfigureAwait(false);

                        if (w < windows.Count)
                        {
                            long chunkStart = bytesWritten;
                            long chunkEnd = bytesWritten + read;
                            while (w < windows.Count)
                            {
                                var ws = windows[w].Item1;
                                var we = windows[w].Item2;
                                if (we <= chunkStart) { w++; continue; }
                                if (ws >= chunkEnd) break;
                                long s = Math.Max(chunkStart, ws);
                                long e = Math.Min(chunkEnd, we);
                                int offset = (int)(s - chunkStart);
                                int count = (int)(e - s);
                                if (count > 0) shaSample.TransformBlock(buffer, offset, count, null, 0);
                                if (we <= chunkEnd) w++; else break;
                            }
                        }

                        bytesWritten += read;
                        progress?.Invoke(bytesWritten, expectedBytes);
                    }

                    shaSample.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                    return new HashCopyResult
                    {
                        SampleHash = BytesToHex(shaSample.Hash),
                        BytesWritten = bytesWritten
                    };
                }
                finally { ArrayPool<byte>.Shared.Return(buffer); }
            }
        }

        private static List<Tuple<long, long>> ComputeSampleWindows(long total, int sampleCount, double samplePct)
        {
            var list = new List<Tuple<long, long>>(Math.Max(1, sampleCount));
            if (total <= 0 || total == long.MaxValue) return list;
            long window = Math.Max(1, (long)Math.Floor(total * samplePct));
            if (window > total) window = total;
            if (sampleCount <= 1)
            {
                list.Add(Tuple.Create(0L, Math.Min(total, window)));
                return list;
            }
            long span = total - window;
            for (int i = 0; i < sampleCount; i++)
            {
                long start = (long)Math.Floor((span * i) / (double)(sampleCount - 1));
                long end = Math.Min(total, start + window);
                list.Add(Tuple.Create(start, end));
            }
            return list;
        }
        #endregion

        #region Utility Helpers
        private static string SafeTrim(string s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();

        private static void ParseSampleSpec(string spec, out int sampleCount, out double samplePct)
        {
            sampleCount = 0; samplePct = 0.0;
            if (string.IsNullOrWhiteSpace(spec)) return;
            try
            {
                var parts = spec.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var p in parts)
                {
                    var kv = p.Split(new[] { '=', ':' }, StringSplitOptions.RemoveEmptyEntries);
                    if (kv.Length == 2)
                    {
                        var k = kv[0].Trim().ToLowerInvariant();
                        var v = kv[1].Trim().ToLowerInvariant();
                        if (k.StartsWith("s"))
                        {
                            var digits = new List<char>();
                            foreach (var ch in v) if (char.IsDigit(ch)) digits.Add(ch);
                            if (int.TryParse(new string(digits.ToArray()), out var x)) sampleCount = x;
                        }
                        else if (k.StartsWith("p"))
                        {
                            if (v.EndsWith("%")) v = v.Substring(0, v.Length - 1);
                            if (double.TryParse(v, System.Globalization.NumberStyles.Any,
                                System.Globalization.CultureInfo.InvariantCulture, out var d))
                                samplePct = (d > 1.0) ? (d / 100.0) : d;
                        }
                    }
                }
            }
            catch { }
        }

        private static string BytesToHex(byte[] bytes)
        {
            if (bytes == null) return null;
            var sb = new StringBuilder(bytes.Length * 2);
            for (int i = 0; i < bytes.Length; i++) sb.Append(bytes[i].ToString("x2"));
            return sb.ToString();
        }

        private static async Task<Dictionary<string, string>> GetNormalizedMetadataAsync(ITusFile file, CancellationToken ct)
        {
            var rawMeta = await file.GetMetadataAsync(ct).ConfigureAwait(false) ?? new Dictionary<string, tusdotnet.Models.Metadata>();
            var meta = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in rawMeta)
            {
                try { meta[kv.Key] = kv.Value == null ? null : kv.Value.GetString(Encoding.UTF8); } catch { }
            }
            return meta;
        }

        private string GetExternalUserId(FileCompleteContext ctx)
        {
            var user = ctx.HttpContext?.User;
            if (user?.Identity?.IsAuthenticated == true) return user.Identity.Name;

            var sys = System.Web.HttpContext.Current;
            var authCookie = sys?.Request?.Cookies[FormsAuthentication.FormsCookieName];
            if (authCookie != null)
            {
                try
                {
                    var ticket = FormsAuthentication.Decrypt(authCookie.Value);
                    if (ticket != null && !string.IsNullOrWhiteSpace(ticket.Name)) return ticket.Name;
                }
                catch (Exception ex) { _log.Warning(ex, "Failed to decrypt FormsAuthentication cookie."); }
            }
            return null;
        }

        private static async Task TryTerminateTusAsync(FileCompleteContext ctx)
        {
            if (ctx.Store is ITusTerminationStore termStore)
            {
                try { await termStore.DeleteFileAsync(ctx.FileId, CancellationToken.None).ConfigureAwait(false); } catch { }
            }
        }

        private async Task<Folder> EnsureUserRootFolderAsync(CloudStorageDbContext db, User user, CancellationToken ct)
        {
            var root = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id, ct).ConfigureAwait(false);
            if (root == null)
            {
                _log.Information("Creating root DB folder for user {User}", user.ExternalUserId);
                root = new Folder { Name = user.ExternalUserId, ParentFolderId = null, OwnerUserId = user.Id, CreatedAt = DateTime.UtcNow };
                db.Folders.Add(root);
                await db.SaveChangesAsync(ct).ConfigureAwait(false);
            }
            return root;
        }

        private async Task<bool> IsValidTargetFolderAsync(CloudStorageDbContext db, int folderId, int userId, CancellationToken ct)
        {
            var candidate = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId, ct).ConfigureAwait(false);
            if (candidate == null) return false;
            var ownerId = await GetRootOwnerIdAsync(db, candidate.Id, ct).ConfigureAwait(false);
            return ownerId == userId;
        }

        private static async Task<int> GetRootOwnerIdAsync(CloudStorageDbContext db, int folderId, CancellationToken ct)
        {
            var current = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId, ct).ConfigureAwait(false);
            if (current == null) return -1;
            while (current.ParentFolderId != null)
            {
                current = await db.Folders.FirstOrDefaultAsync(f => f.Id == current.ParentFolderId.Value, ct).ConfigureAwait(false);
                if (current == null) return -1;
            }
            return current.OwnerUserId ?? -1;
        }

        private static async Task<int> EnsureFolderHierarchyAsync(CloudStorageDbContext db, int ownerUserId, int baseFolderId, string relativeFolderPath, CancellationToken ct)
        {
            var segments = (relativeFolderPath ?? string.Empty).Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            int currentParentId = baseFolderId;

            foreach (var segment in segments)
            {
                var existing = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == currentParentId && f.Name.Equals(segment, StringComparison.OrdinalIgnoreCase), ct).ConfigureAwait(false);
                if (existing != null) currentParentId = existing.Id;
                else
                {
                    var newFolder = new Folder { Name = segment, ParentFolderId = currentParentId, OwnerUserId = ownerUserId, CreatedAt = DateTime.UtcNow };
                    db.Folders.Add(newFolder);
                    await db.SaveChangesAsync(ct).ConfigureAwait(false);
                    currentParentId = newFolder.Id;
                }
            }
            return currentParentId;
        }

        private static int? TryParseInt(string s) { return int.TryParse(s, out var n) ? (int?)n : null; }

        private static void TryDeleteFile(string path)
        {
            try { if (!string.IsNullOrWhiteSpace(path) && File.Exists(path)) File.Delete(path); } catch { }
        }

        private static string MakeUniqueName(string dir, string desiredName)
        {
            var baseName = Path.GetFileNameWithoutExtension(desiredName);
            var ext = Path.GetExtension(desiredName);
            var finalName = desiredName;
            int i = 1;
            while (ExtendedFileExists(Path.Combine(dir, finalName))) finalName = baseName + "(" + (i++) + ")" + ext;
            return finalName;
        }

        private static void EnsureDirectoryForFile(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("Final path is empty", nameof(filePath));
            var dir = Path.GetDirectoryName(filePath);
            if (string.IsNullOrWhiteSpace(dir))
                throw new InvalidOperationException($"Final path has no directory: {filePath}");
            ExtendedCreateDirectory(dir);
        }

        private static string NormalizeRelativeDir(string relativePath)
        {
            if (string.IsNullOrWhiteSpace(relativePath)) return null;

            var norm = relativePath.Replace('\\', '/').Trim();
            norm = norm.Trim('/');

            var parts = norm.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries).ToList();
            if (parts.Count == 0) return null;

            parts.RemoveAt(parts.Count - 1);

            var clean = new List<string>(parts.Count);
            foreach (var p in parts)
            {
                if (p == ".") continue;
                if (p == "..")
                {
                    if (clean.Count > 0) clean.RemoveAt(clean.Count - 1);
                    continue;
                }
                clean.Add(p);
            }

            if (clean.Count == 0) return null;
            return string.Join("/", clean);
        }

        // Long-path helpers
        private static bool PathIsLikelyTooLong(string path)
            => !string.IsNullOrEmpty(path) && (path.Length >= MAX_PATH || (Path.GetDirectoryName(path)?.Length ?? 0) >= MAX_DIR);

        private static bool IsUncPath(string fullPath) => fullPath.StartsWith(@"\\", StringComparison.Ordinal);

        private static string ToExtendedPath(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) return path;

            var full = Path.GetFullPath(path);
            if (IsUncPath(full))
            {
                return @"\\?\UNC" + full.Substring(1);
            }
            return @"\\?\" + full;
        }

        private static bool ExtendedDirectoryExists(string path)
        {
            try
            {
                if (!PathIsLikelyTooLong(path)) return Directory.Exists(path);
                return Directory.Exists(ToExtendedPath(path));
            }
            catch { return false; }
        }

        private static void ExtendedCreateDirectory(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return;

            try
            {
                if (!PathIsLikelyTooLong(path))
                {
                    if (!Directory.Exists(path)) Directory.CreateDirectory(path);
                    return;
                }

                var ext = ToExtendedPath(path);
                if (!Directory.Exists(ext))
                {
                    Directory.CreateDirectory(ext);
                }
            }
            catch (PathTooLongException)
            {
                var full = Path.GetFullPath(path);
                var root = Path.GetPathRoot(full);
                var remainder = full.Substring(root.Length).Split(Path.DirectorySeparatorChar);

                var current = root.TrimEnd(Path.DirectorySeparatorChar);
                foreach (var segment in remainder)
                {
                    current = current + Path.DirectorySeparatorChar + segment;
                    var curExt = ToExtendedPath(current);
                    if (!Directory.Exists(curExt))
                    {
                        Directory.CreateDirectory(curExt);
                    }
                }
            }
        }

        private static bool ExtendedFileExists(string path)
        {
            try
            {
                if (!PathIsLikelyTooLong(path)) return File.Exists(path);
                return File.Exists(ToExtendedPath(path));
            }
            catch { return false; }
        }

        private static void ExtendedFileDelete(string path)
        {
            try
            {
                if (!PathIsLikelyTooLong(path)) { File.Delete(path); return; }
                File.Delete(ToExtendedPath(path));
            }
            catch { }
        }

        private static void ExtendedFileMove(string source, string dest)
        {
            try
            {
                if (!PathIsLikelyTooLong(source) && !PathIsLikelyTooLong(dest))
                {
                    File.Move(source, dest);
                    return;
                }
            }
            catch (DirectoryNotFoundException)
            {
                // Will retry with extended path below
            }

            var src = PathIsLikelyTooLong(source) ? ToExtendedPath(source) : source;
            var dst = PathIsLikelyTooLong(dest) ? ToExtendedPath(dest) : dest;

            File.Move(src, dst);
        }
        #endregion
    }
}