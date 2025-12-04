using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Web.Hosting;

namespace BOBDrive.Services
{
    /// <summary>
    /// Central service to tail application logs in near-real-time.
    /// - Keeps a ring buffer of recent lines.
    /// - Periodically polls the log files for appended content.
    /// - Thread-safe and AppDomain-safe (reinitializes paths via LoggingConfig-style resolution).
    /// </summary>
    public static class LogTailService
    {
        private class LogBuffer
        {
            public string LogicalName { get; set; }        // "upload", "audit"
            public string FilePattern { get; set; }        // e.g. "upload-log-*.txt"
            public string ResolvedDirectory { get; set; }  // physical directory
            public int MaxLines { get; set; } = 5000;

            public readonly LinkedList<string> Lines = new LinkedList<string>();
            public readonly object Sync = new object();

            // State for polling
            public string CurrentFilePath;
            public long LastLength;
            public DateTime LastScanUtc;
        }

        // Two main logs: upload and audit
        private static readonly ConcurrentDictionary<string, LogBuffer> _buffers =
            new ConcurrentDictionary<string, LogBuffer>(StringComparer.OrdinalIgnoreCase);

        private static readonly Timer _timer;
        private static readonly TimeSpan _pollInterval = TimeSpan.FromSeconds(1.0);

        static LogTailService()
        {
            // Initialize known logs based on LoggingConfig conventions
            var baseDir = ResolveAppDataLogsDirectory();
            EnsureDirectory(baseDir);

            _buffers["upload"] = new LogBuffer
            {
                LogicalName = "upload",
                FilePattern = "upload-log-*.txt",
                ResolvedDirectory = baseDir,
                MaxLines = 8000
            };

            _buffers["audit"] = new LogBuffer
            {
                LogicalName = "audit",
                FilePattern = "audit-sharelink-log-*.txt",
                ResolvedDirectory = baseDir,
                MaxLines = 8000
            };

            // Initial snapshot so first viewer gets some history
            foreach (var buffer in _buffers.Values)
            {
                try { RefreshBuffer(buffer, initialSnapshot: true); } catch { /* swallow */ }
            }

            _timer = new Timer(_ => Tick(), null, _pollInterval, _pollInterval);
        }

        private static string ResolveAppDataLogsDirectory()
        {
            string appDataPath = null;
            try { appDataPath = HostingEnvironment.MapPath("~/App_Data"); } catch { }
            if (string.IsNullOrEmpty(appDataPath))
            {
                appDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data");
            }
            var logsPath = Path.Combine(appDataPath, "Logs");
            return logsPath;
        }

        private static void EnsureDirectory(string path)
        {
            try
            {
                if (!Directory.Exists(path)) Directory.CreateDirectory(path);
            }
            catch { /* ignore */ }
        }

        private static void Tick()
        {
            foreach (var kvp in _buffers)
            {
                try
                {
                    RefreshBuffer(kvp.Value, initialSnapshot: false);
                }
                catch
                {
                    // Swallow; logging the logger is not helpful here.
                }
            }
        }

        private static void RefreshBuffer(LogBuffer buffer, bool initialSnapshot)
        {
            var now = DateTime.UtcNow;
            // Simple throttle
            if (!initialSnapshot && (now - buffer.LastScanUtc) < _pollInterval.Subtract(TimeSpan.FromMilliseconds(200)))
                return;

            buffer.LastScanUtc = now;

            if (!Directory.Exists(buffer.ResolvedDirectory))
                return;

            var dir = new DirectoryInfo(buffer.ResolvedDirectory);
            var file = dir.GetFiles(buffer.FilePattern)
              .OrderByDescending(f => GetDateFromFileName(f, buffer.LogicalName))
              .ThenByDescending(f => f.LastWriteTimeUtc)
              .FirstOrDefault();
            if (file == null || !file.Exists)
                return;

            var path = file.FullName;
            if (!string.Equals(path, buffer.CurrentFilePath, StringComparison.OrdinalIgnoreCase))
            {
                // New log file (rolled) – reset state and read tail
                buffer.CurrentFilePath = path;
                buffer.LastLength = 0;
                if (initialSnapshot)
                {
                    var lines = ReadLastLines(path, buffer.MaxLines);
                    lock (buffer.Sync)
                    {
                        buffer.Lines.Clear();
                        foreach (var line in lines) buffer.Lines.AddLast(line);
                        TrimBuffer(buffer);
                    }
                    buffer.LastLength = new FileInfo(path).Length;
                }
                return;
            }

            var info = new FileInfo(path);
            if (!info.Exists) return;

            if (info.Length < buffer.LastLength)
            {
                // Log truncated – resync
                buffer.LastLength = 0;
            }

            if (info.Length == buffer.LastLength)
                return; // nothing new

            var newLines = ReadNewLines(path, buffer.LastLength);
            buffer.LastLength = info.Length;

            if (newLines.Count > 0)
            {
                lock (buffer.Sync)
                {
                    foreach (var l in newLines) buffer.Lines.AddLast(l);
                    TrimBuffer(buffer);
                }
            }
        }

        private static void TrimBuffer(LogBuffer buffer)
        {
            while (buffer.Lines.Count > buffer.MaxLines)
                buffer.Lines.RemoveFirst();
        }

        private static List<string> ReadLastLines(string path, int maxLines)
        {
            var result = new List<string>();
            try
            {
                // Efficient backwards read
                const int bufferSize = 8192;
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    long filePos = fs.Length;
                    int remaining = maxLines;
                    var sb = new StringBuilder();

                    while (filePos > 0 && remaining > 0)
                    {
                        int toRead = (int)Math.Min(bufferSize, filePos);
                        filePos -= toRead;
                        fs.Position = filePos;

                        var buf = new byte[toRead];
                        fs.Read(buf, 0, toRead);
                        for (int i = toRead - 1; i >= 0; i--)
                        {
                            char c = (char)buf[i];
                            if (c == '\n')
                            {
                                var line = new string(sb.ToString().Reverse().ToArray());
                                if (!string.IsNullOrEmpty(line))
                                {
                                    result.Add(line.TrimEnd('\r'));
                                    remaining--;
                                    sb.Clear();
                                    if (remaining == 0) break;
                                }
                            }
                            else
                            {
                                sb.Append(c);
                            }
                        }
                    }

                    if (sb.Length > 0 && remaining > 0)
                    {
                        var line = new string(sb.ToString().Reverse().ToArray());
                        result.Add(line.TrimEnd('\r'));
                    }
                }
            }
            catch
            {
                // If anything fails, just fall back to File.ReadLines
                try
                {
                    result = File.ReadLines(path).Reverse().Take(maxLines).Reverse().ToList();
                }
                catch
                {
                    result = new List<string>();
                }
            }

            return result;
        }

        private static List<string> ReadNewLines(string path, long startOffset)
        {
            var list = new List<string>();
            try
            {
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    if (startOffset > fs.Length) startOffset = 0;
                    fs.Position = startOffset;
                    using (var sr = new StreamReader(fs, Encoding.UTF8, true, 4096, leaveOpen: true))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            list.Add(line);
                        }
                    }
                }
            }
            catch
            {
                // swallow
            }
            return list;
        }

        /// <summary>
        /// Gets the snapshot (last N lines) for a named log: "upload" or "audit".
        /// </summary>
        public static IList<string> GetSnapshot(string logicalName, int maxLines)
        {
            if (!_buffers.TryGetValue(logicalName ?? "", out var buffer))
                return Array.Empty<string>();

            lock (buffer.Sync)
            {
                if (buffer.Lines.Count == 0)
                    return Array.Empty<string>();

                int take = maxLines <= 0
                    ? buffer.Lines.Count        // full buffer
                    : Math.Min(maxLines, buffer.Lines.Count);

                return buffer.Lines.Reverse().Take(take).ToList();
            }
        }


        private static DateTime GetDateFromFileName(FileInfo file, string logicalName)
        {
            // Assumes pattern: upload-log-YYYYMMDD*.txt or audit-sharelink-log-YYYYMMDD*.txt
            // Fallback to LastWriteTimeUtc if parse fails.
            var name = file.Name;
            var match = System.Text.RegularExpressions.Regex.Match(name, @"(\d{8})(?=\D*\.txt$)");
            if (match.Success)
            {
                var y = int.Parse(match.Groups[1].Value.Substring(0, 4));
                var m = int.Parse(match.Groups[1].Value.Substring(4, 2));
                var d = int.Parse(match.Groups[1].Value.Substring(6, 2));
                try
                {
                    return new DateTime(y, m, d, 0, 0, 0, DateTimeKind.Utc);
                }
                catch
                {
                    // ignore and fall back
                }
            }
            return file.LastWriteTimeUtc;
        }

        internal static DateTime? TryParseTimestamp(string line)
        {
            if (string.IsNullOrWhiteSpace(line)) return null;

            // Serilog template: 2025-11-18 06:01:40.123 +00:00 [...]
            var length = Math.Min(33, line.Length);
            var tsStr = line.Substring(0, length).Trim();
            if (DateTimeOffset.TryParse(tsStr,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out var dto))
            {
                return dto.UtcDateTime;
            }
            return null;
        }


        // NEW: list all log files for a logical log (upload/audit), newest first
        public static IList<FileInfo> GetAllLogFiles(string logicalName)
        {
            if (!_buffers.TryGetValue(logicalName ?? "", out var buffer))
                return Array.Empty<FileInfo>();

            if (!Directory.Exists(buffer.ResolvedDirectory))
                return Array.Empty<FileInfo>();

            var dir = new DirectoryInfo(buffer.ResolvedDirectory);
            return dir.GetFiles(buffer.FilePattern)
          .OrderByDescending(f => GetDateFromFileName(f, logicalName))
          .ThenByDescending(f => f.LastWriteTimeUtc)
          .ToList();
        }

        public static IList<string> GetFileSnapshot(string logicalName, string fileName, int maxLines)
        {
            if (string.IsNullOrWhiteSpace(logicalName) || string.IsNullOrWhiteSpace(fileName))
                return Array.Empty<string>();

            if (!_buffers.TryGetValue(logicalName, out var buffer))
                return Array.Empty<string>();

            var dir = new DirectoryInfo(buffer.ResolvedDirectory);
            if (!dir.Exists) return Array.Empty<string>();

            var file = dir.GetFiles(buffer.FilePattern)
                          .FirstOrDefault(f => f.Name.Equals(fileName, StringComparison.OrdinalIgnoreCase));
            if (file == null || !file.Exists) return Array.Empty<string>();

            int limit = maxLines <= 0
                ? int.MaxValue                   // full file
                : Math.Min(maxLines, buffer.MaxLines);

            return ReadLastLines(file.FullName, limit);
        }


        /// <summary>
        /// Returns the names of all known logs.
        /// </summary>
        public static IEnumerable<string> GetLogNames()
        {
            return _buffers.Keys.ToArray();
        }

        /// <summary>
        /// Returns the current physical file path (if any) for a logical log.
        /// </summary>
        public static string GetCurrentFilePath(string logicalName)
        {
            if (!_buffers.TryGetValue(logicalName ?? "", out var buffer))
                return null;
            return buffer.CurrentFilePath;
        }
    }
}