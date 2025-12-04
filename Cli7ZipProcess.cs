using BOBDrive.App_Start;
using Serilog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace BOBDrive.Controllers
{
    /// <summary>
    /// Thin wrapper around the 7-Zip CLI with robust progress parsing and stall detection.
    /// All existing methods and behaviors are preserved. New helper methods are added to support:
    ///  - Direct single-file compression without staging (CompressSingleFileAsync)
    ///  - Compression from a list file (CompressFromListFileAsync)
    ///  - Appending multiple entries via -si to avoid staging (AppendEntriesWithStdinAsync)
    /// </summary>
    public class Cli7ZipProcess
    {
        private static readonly ILogger _log = Log.ForContext<Cli7ZipProcess>();
        private readonly string _sevenZipExePath;

        // Matches NN% anywhere in the stream
        private static readonly Regex _progressRegex = new Regex(@"(\d{1,3})%", RegexOptions.Compiled);

        // Stall detection: if no new progress seen for this duration, kill 7z to avoid hanging
        private readonly TimeSpan ProgressStallTimeout;

        // Emit more output lines to reduce false stall detection
        private readonly bool _zipVerboseOutput;

        public Cli7ZipProcess()
        {
            _sevenZipExePath = UploadConfiguration.SevenZipExePath;
            _log.Debug("ZIP_STREAM: 7-Zip path resolved to {Path}", _sevenZipExePath);

            // Configurable stall timeout (minutes); default 30
            int minutes = 30;
            var cfgMinutes = ConfigurationManager.AppSettings["ZipProgressStallTimeoutMinutes"];
            if (!string.IsNullOrWhiteSpace(cfgMinutes) && int.TryParse(cfgMinutes, out var m) && m > 0)
            {
                minutes = m;
            }
            ProgressStallTimeout = TimeSpan.FromMinutes(minutes);

            // Configurable verbosity; default true
            var cfgVerbose = ConfigurationManager.AppSettings["ZipVerboseOutput"];
            _zipVerboseOutput = string.IsNullOrWhiteSpace(cfgVerbose)
                ? true
                : cfgVerbose.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Compress multiple files in one go by invoking 7-Zip and passing the file paths directly.
        /// Preserves existing behavior. Starts zipping immediately if the files are accessible.
        /// </summary>
        public Task CompressFilesAsync(string[] sourceFilePaths,
                                       string destinationArchivePath,
                                       string password,
                                       Action<int> progressCallback,
                                       CancellationToken cancellationToken = default)
        {
            return Task.Run(() =>
            {
                _log.Information("ZIP_FILES: Starting multi-file compression to {Dest}", destinationArchivePath);
                Ensure7ZipExists();

                string filesToArchive = string.Join(" ", sourceFilePaths.Select(p => $"\"{p}\""));
                // -bsp2 => total & current progress, -bb1 => more output lines (helps avoid false stalls)
                string arguments = $"a -tzip \"{destinationArchivePath}\" {filesToArchive} -p\"{EscapePassword(password)}\" -bsp2"
                    + (_zipVerboseOutput ? " -bb1" : "");

                Run7ZipWithConcurrentIO(arguments, progressCallback, null, null, cancellationToken);
            }, cancellationToken);
        }

        /// <summary>
        /// Compress an entire directory (non-recursive path in args with working dir set to parent, so paths inside ZIP are relative).
        /// Preserves existing behavior.
        /// </summary>
        public Task CompressDirectoryAsync(string sourceDirectory,
                                           string destinationArchivePath,
                                           string password,
                                           Action<int> progressCallback,
                                           CancellationToken cancellationToken = default)
        {
            return Task.Run(() =>
            {
                _log.Information("ZIP_DIR: Starting directory compression {Src} -> {Dest}", sourceDirectory, destinationArchivePath);
                Ensure7ZipExists();
                if (!Directory.Exists(sourceDirectory))
                    throw new DirectoryNotFoundException("Source directory not found: " + sourceDirectory);

                // Run 7z in the parent dir so relative paths are stored nicely
                string parent = Directory.GetParent(sourceDirectory).FullName;
                string folderName = new DirectoryInfo(sourceDirectory).Name;

                string arguments = $"a -tzip \"{destinationArchivePath}\" \"{folderName}\\*\" -p\"{EscapePassword(password)}\" -bsp2"
                    + (_zipVerboseOutput ? " -bb1" : "");

                Run7ZipWithConcurrentIO(arguments,
                                        progressCallback,
                                        stdinWriterAsync: null,
                                        workingDirectory: parent,
                                        cancellationToken: cancellationToken);
            }, cancellationToken);
        }

        /// <summary>
        /// Streaming compression (single assembled logical file) — feeds concatenated chunks via stdin.
        /// Preserves existing behavior and is ideal for creating a ZIP that contains one large entry.
        /// </summary>
        // REPLACE this method in Cli7ZipProcess
        public Task CompressStreamedAsync(
            string[] chunkPaths,
            string destinationArchivePath,
            string entryFileName,
            string password,
            Action<int> progressCallback,
            CancellationToken cancellationToken = default,
            Action<int> onProcessStarted = null // NEW optional callback: invoked with 7z PID
        )
        {
            return Task.Run(() =>
            {
                _log.Information("ZIP_STREAM: Starting streamed compression to {Dest} (Entry: {Entry})",
                    destinationArchivePath, entryFileName);
                Ensure7ZipExists();

                string arguments = $"a -tzip -si\"{entryFileName}\" \"{destinationArchivePath}\" -p\"{EscapePassword(password)}\" -bsp2"
                    + (_zipVerboseOutput ? " -bb1" : "");

                Func<Stream, Task> writer = async stdin =>
                {
                    long totalBytes = 0;
                    try { totalBytes = chunkPaths.Sum(p => new FileInfo(p).Length); }
                    catch (Exception ex) { _log.Warning(ex, "ZIP_STREAM: Failed to sum chunk sizes."); }

                    long written = 0;
                    var buffer = new byte[1024 * 1024]; // 1 MB
                    int lastReported = -1;
                    int chunkIndex = 0;

                    foreach (var chunkPath in chunkPaths)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _log.Warning("ZIP_STREAM: Cancellation requested mid-stream.");
                            break;
                        }

                        if (!File.Exists(chunkPath))
                            throw new FileNotFoundException("Chunk missing on disk during streaming", chunkPath);

                        using (var fs = new FileStream(chunkPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                        {
                            int read;
                            while ((read = await fs.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                            {
                                await stdin.WriteAsync(buffer, 0, read, cancellationToken);
                                written += read;

                                if (totalBytes > 0)
                                {
                                    int pct = (int)(written * 100 / totalBytes);
                                    if (pct > 100) pct = 100;
                                    if (pct != lastReported)
                                    {
                                        lastReported = pct;
                                        try { progressCallback?.Invoke(pct); } catch { /* ignore */ }
                                    }
                                }
                            }
                        }

                        chunkIndex++;
                        _log.Debug("ZIP_STREAM: Finished feeding chunk {Idx}/{Total}", chunkIndex, chunkPaths.Length);
                    }

                    await stdin.FlushAsync(cancellationToken);
                    stdin.Close();
                    _log.Debug("ZIP_STREAM: All chunks written ({Bytes} bytes). STDIN closed.", written);
                };

                Run7ZipWithConcurrentIO(
                    arguments,
                    progressCallback,
                    stdinWriterAsync: writer,
                    workingDirectory: null,
                    cancellationToken: cancellationToken,
                    onProcessStarted: onProcessStarted // pass through so caller can record PID
                );
            }, cancellationToken);
        }

        // =========================
        // NEW: Helper methods
        // =========================

        /// <summary>
        /// NEW: Direct single-file compression with no staging. Uses the file's directory as the working directory
        /// so that only the base name is stored in the archive (better UX on extraction).
        /// </summary>
        public Task CompressSingleFileAsync(string sourceFilePath,
                                            string destinationArchivePath,
                                            string password,
                                            Action<int> progressCallback,
                                            CancellationToken cancellationToken = default)
        {
            return Task.Run(() =>
            {
                _log.Information("ZIP_FILE: Starting single-file compression {Src} -> {Dest}",
                    sourceFilePath, destinationArchivePath);

                Ensure7ZipExists();

                var dir = Path.GetDirectoryName(sourceFilePath);
                var name = Path.GetFileName(sourceFilePath);
                if (string.IsNullOrEmpty(dir) || string.IsNullOrEmpty(name))
                    throw new ArgumentException("Invalid source file path: " + sourceFilePath);

                string arguments =
                    $"a -tzip \"{destinationArchivePath}\" \"{name}\" -p\"{EscapePassword(password)}\" -bsp2"
                    + (_zipVerboseOutput ? " -bb1" : "");

                Run7ZipWithConcurrentIO(arguments,
                                        progressCallback,
                                        stdinWriterAsync: null,
                                        workingDirectory: dir,
                                        cancellationToken: cancellationToken);
            }, cancellationToken);
        }

        /// <summary>
        /// NEW: Compress using a list file that enumerates all files to include (one per line).
        /// If you include absolute paths in the list, pass includeAbsolutePaths=true to add -spf2.
        /// </summary>
        // CHANGE: Add onProcessStarted parameter and pass it to the runner so PID can be recorded.
        // REPLACE this entire method with the version below
        public Task CompressFromListFileAsync(string listFilePath,
                                              string destinationArchivePath,
                                              string password,
                                              string workingDirectory = null,
                                              bool includeAbsolutePaths = true,
                                              Action<int> progressCallback = null,
                                              CancellationToken cancellationToken = default,
                                              Action<int> onProcessStarted = null)
        {
            return Task.Run(() =>
            {
                _log.Information("ZIP_LIST: Starting compression from list {List} -> {Dest}", listFilePath, destinationArchivePath);
                Ensure7ZipExists();
                if (!File.Exists(listFilePath)) throw new FileNotFoundException("List file not found", listFilePath);

                // IMPORTANT: quote the @listFilePath and set charset explicitly
                var sb = new StringBuilder();
                sb.Append($"a -tzip \"{destinationArchivePath}\" @\"{listFilePath}\" -p\"{EscapePassword(password)}\" -bsp2 -scsUTF-8");
                if (_zipVerboseOutput) sb.Append(" -bb1");
                if (includeAbsolutePaths) sb.Append(" -spf2");

                Run7ZipWithConcurrentIO(sb.ToString(),
                                        progressCallback,
                                        stdinWriterAsync: null,
                                        workingDirectory: string.IsNullOrWhiteSpace(workingDirectory) ? null : workingDirectory,
                                        cancellationToken: cancellationToken,
                                        onProcessStarted: onProcessStarted);
            }, cancellationToken);
        }

        /// <summary>
        /// NEW: Append multiple entries via stdin, one per 7z invocation,
        /// to avoid pre-staging while keeping full control of entry names in the ZIP.
        /// Suitable for tens/hundreds of large files. For thousands of tiny files, prefer list file or directory compression.
        /// </summary>
        // REPLACE this method in Cli7ZipProcess
        // CHANGE: Add entryCompletedCallback parameter and invoke it after each entry is appended.
        public async Task AppendEntriesWithStdinAsync(
            string destinationArchivePath,
            string password,
            IEnumerable<(string SourcePath, string EntryName, long Size)> entries,
            Action<int> overallProgressCallback,
            CancellationToken cancellationToken = default,
            Action<int> onProcessStarted = null,
            Action<int> entryCompletedCallback = null)
        {
            Ensure7ZipExists();

            long totalBytes = entries.Sum(e => Math.Max(0, e.Size));
            if (totalBytes <= 0)
            {
                try { totalBytes = entries.Select(e => new FileInfo(e.SourcePath)).Where(fi => fi.Exists).Sum(fi => fi.Length); } catch { }
            }
            totalBytes = Math.Max(1, totalBytes);

            long writtenTotal = 0;
            int lastReported = -1;
            int processedCount = 0;

            foreach (var entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!File.Exists(entry.SourcePath))
                    throw new FileNotFoundException("Source not found", entry.SourcePath);

                string arguments =
                    $"a -tzip -si\"{entry.EntryName}\" \"{destinationArchivePath}\" -p\"{EscapePassword(password)}\" -bsp2"
                    + (_zipVerboseOutput ? " -bb1" : "");

                _log.Information("ZIP_MULTI_SI: Appending {Entry} from {Src}", entry.EntryName, entry.SourcePath);

                await Task.Run(() =>
                {
                    Run7ZipWithConcurrentIO(
                        arguments,
                        progressCallback: null,
                        stdinWriterAsync: async stdin =>
                        {
                            var buffer = new byte[1024 * 1024]; // 1MB
                            using (var fs = new FileStream(entry.SourcePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                            {
                                int read;
                                while ((read = await fs.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                                {
                                    await stdin.WriteAsync(buffer, 0, read, cancellationToken);
                                    writtenTotal += read;

                                    int pct = (int)(writtenTotal * 100 / totalBytes);
                                    if (pct > 100) pct = 100;
                                    if (pct != lastReported)
                                    {
                                        lastReported = pct;
                                        try { overallProgressCallback?.Invoke(pct); } catch { /* ignore */ }
                                    }
                                }
                            }
                            await stdin.FlushAsync(cancellationToken);
                            stdin.Close();
                        },
                        workingDirectory: null,
                        cancellationToken: cancellationToken,
                        onProcessStarted: onProcessStarted
                    );
                }, cancellationToken);

                processedCount++;
                try { entryCompletedCallback?.Invoke(processedCount); } catch { /* ignore */ }
            }

            if (lastReported < 100) { try { overallProgressCallback?.Invoke(100); } catch { } }
        }





        // =========================
        // Core runner and helpers
        // =========================

        // REPLACE this method in Cli7ZipProcess
        private void Run7ZipWithConcurrentIO(
            string arguments,
            Action<int> progressCallback,
            Func<Stream, Task> stdinWriterAsync = null,
            string workingDirectory = null,
            CancellationToken cancellationToken = default,
            Action<int> onProcessStarted = null // NEW optional callback to expose 7z PID
        )
        {
            var psi = new ProcessStartInfo
            {
                FileName = _sevenZipExePath,
                Arguments = arguments,
                RedirectStandardInput = (stdinWriterAsync != null),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8
            };
            if (!string.IsNullOrWhiteSpace(workingDirectory))
                psi.WorkingDirectory = workingDirectory;

            using (var process = new Process { StartInfo = psi, EnableRaisingEvents = true })
            {
                var lastProgressChange = DateTime.UtcNow;
                int lastReportedProgress = -1;
                object progressLock = new object();

                void MarkActivity() => lastProgressChange = DateTime.UtcNow;

                _log.Information("ZIP_CORE: Launching 7z: {Exe} {Args}", psi.FileName, psi.Arguments);

                if (!process.Start())
                    throw new InvalidOperationException("Failed to start 7-Zip process.");

                // NEW: immediately expose PID to caller so they can persist it
                try { onProcessStarted?.Invoke(process.Id); } catch (Exception cbEx) { _log.Warning(cbEx, "ZIP_CORE: onProcessStarted threw."); }

                Task writerTask = Task.CompletedTask;
                if (stdinWriterAsync != null)
                {
                    var baseStream = process.StandardInput.BaseStream;
                    writerTask = Task.Run(async () =>
                    {
                        try
                        {
                            await stdinWriterAsync(baseStream);
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex, "ZIP_CORE: Exception while writing to stdin.");
                            try { baseStream.Close(); } catch { }
                            throw;
                        }
                    }, cancellationToken);
                }
                else
                {
                    try { process.StandardInput.Close(); } catch { }
                }

                var stdOutTask = ReadAndParseProgressAsync(process.StandardOutput.BaseStream, false, SafeReport, MarkActivity, cancellationToken);
                var stdErrTask = ReadAndParseProgressAsync(process.StandardError.BaseStream, true, SafeReport, MarkActivity, cancellationToken);

                Task watchdog = Task.Run(async () =>
                {
                    while (!process.HasExited)
                    {
                        try { await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken); }
                        catch { /* ignore */ }

                        if (cancellationToken.IsCancellationRequested) break;

                        var idle = DateTime.UtcNow - lastProgressChange;
                        if (idle > ProgressStallTimeout)
                        {
                            try
                            {
                                _log.Error("ZIP_CORE: Stall detected (> {Timeout}). Killing process PID={Pid}.", ProgressStallTimeout, process.Id);
                                process.Kill();
                            }
                            catch (Exception killEx)
                            {
                                _log.Warning(killEx, "ZIP_CORE: Failed to kill stalled process.");
                            }
                            break;
                        }
                    }
                }, cancellationToken);

                Task waitForExit = Task.Run(() => process.WaitForExit(), cancellationToken);

                Task.WhenAll(writerTask, stdOutTask, stdErrTask, waitForExit, watchdog).GetAwaiter().GetResult();

                if (process.ExitCode == 0 && lastReportedProgress < 100)
                    SafeReport(100);

                if (process.ExitCode != 0)
                    throw new Exception($"7-Zip failed (exit code {process.ExitCode}).");

                _log.Information("ZIP_CORE: 7z completed successfully.");

                void SafeReport(int pct)
                {
                    lock (progressLock)
                    {
                        if (pct < 0 || pct > 100) return;
                        if (pct == lastReportedProgress) return;
                        lastReportedProgress = pct;
                        lastProgressChange = DateTime.UtcNow;
                    }
                    try { progressCallback?.Invoke(pct); }
                    catch (Exception cbEx) { _log.Warning(cbEx, "ZIP_CORE: Progress callback threw."); }
                }
            }
        }



        private async Task ReadAndParseProgressAsync(Stream baseStream,
                                                     bool isError,
                                                     Action<int> report,
                                                     Action markActivity,
                                                     CancellationToken ct)
        {
            // Read raw, handle CR and newline-less updates
            var buffer = new byte[4096];
            var sb = new StringBuilder(8192);

            void scan()
            {
                var text = sb.ToString();
                var matches = _progressRegex.Matches(text);
                if (matches.Count > 0)
                {
                    // take last match as latest progress
                    if (int.TryParse(matches[matches.Count - 1].Groups[1].Value, out int pct))
                    {
                        report(pct);
                    }
                }
                // prevent unbounded growth: trim older content
                if (sb.Length > 8192) // keep tail
                {
                    var tail = sb.ToString(sb.Length - 2048, 2048);
                    sb.Clear();
                    sb.Append(tail);
                }
            }

            while (!ct.IsCancellationRequested)
            {
                int read;
                try
                {
                    read = await baseStream.ReadAsync(buffer, 0, buffer.Length, ct);
                }
                catch
                {
                    break; // stream ended or process exited
                }

                if (read <= 0) break;

                markActivity();

                var chunk = Encoding.UTF8.GetString(buffer, 0, read);
                sb.Append(chunk);

                // Log chunks at debug level (avoid line-based dependency)
                if (isError) _log.Debug("ZIP_STDERR: {Chunk}", chunk);
                else _log.Debug("ZIP_STDOUT: {Chunk}", chunk);

                scan();
            }
        }

        private void Ensure7ZipExists()
        {
            if (!File.Exists(_sevenZipExePath))
                throw new FileNotFoundException("7-Zip executable not found", _sevenZipExePath);
        }

        private static string EscapePassword(string pwd) =>
            (pwd ?? "").Replace("\"", "\\\"");
    }
}

