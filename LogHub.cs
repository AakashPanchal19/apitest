using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using BOBDrive.Services;

namespace BOBDrive.Hubs
{
    [Authorize]
    public class LogHub : Hub
    {
        // Keep a default only for callers that omit maxLines
        private const int DEFAULT_SNAPSHOT_LINES = 500;

        public override Task OnConnected()
        {
            var logs = LogTailService.GetLogNames().Select(n =>
            {
                var files = LogTailService.GetAllLogFiles(n)
                    .Select(f => new
                    {
                        Name = f.Name,
                        FullPath = f.FullName,
                        LastWriteUtc = f.LastWriteTimeUtc
                    })
                    .ToArray();

                return new
                {
                    Name = n,
                    CurrentFile = LogTailService.GetCurrentFilePath(n),
                    Files = files
                };
            }).ToArray();

            Clients.Caller.initialize(logs);
            return base.OnConnected();
        }

        /// <summary>
        /// Tail of logical log (latest file). 
        /// maxLines &lt;= 0 means "no limit" (full in-memory buffer) and is passed through.
        /// </summary>
        public void RequestSnapshot(string logName, int maxLines = DEFAULT_SNAPSHOT_LINES)
        {
            logName = (logName ?? "").ToLowerInvariant();

            // Do NOT normalize <= 0; LogTailService treats <= 0 as "full".
            var lines = LogTailService.GetSnapshot(logName, maxLines);
            Clients.Caller.snapshot(logName, lines);
        }

        /// <summary>
        /// Periodic poll for logical log (latest file).
        /// maxLines &lt;= 0 means "no limit".
        /// </summary>
        public void PollLog(string logName, int maxLines = DEFAULT_SNAPSHOT_LINES)
        {
            logName = (logName ?? "").ToLowerInvariant();

            var lines = LogTailService.GetSnapshot(logName, maxLines);
            Clients.Caller.update(logName, lines);
        }

        /// <summary>
        /// Get a specific file's content. 
        /// maxLines &lt;= 0 means "no limit" (full file).
        /// </summary>
        public void GetFileSnapshot(string logName, string fileName, int maxLines = DEFAULT_SNAPSHOT_LINES)
        {
            logName = (logName ?? "").ToLowerInvariant();

            var lines = LogTailService.GetFileSnapshot(logName, fileName, maxLines);
            Clients.Caller.fileSnapshot(logName, fileName, lines);
        }

        /// <summary>
        /// Returns the latest log file name (by date/LastWrite) for a given logical log.
        /// </summary>
        public string GetLatestFileName(string logName)
        {
            logName = (logName ?? "").ToLowerInvariant();
            var all = LogTailService.GetAllLogFiles(logName);
            var latest = all.FirstOrDefault();
            return latest?.Name;
        }
    }
}