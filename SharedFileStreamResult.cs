using Serilog;
using System;
using System.IO;
using System.Web;
using System.Web.Mvc;

namespace BOBDrive.Controllers
{
    /// <summary>
    /// Custom ActionResult that streams files with FileShare.Read to allow concurrent downloads.
    /// Critical for pooled deduplication where multiple DB records reference the same physical file.
    /// </summary>
    public class SharedFileStreamResult : ActionResult
    {
        private readonly string _filePath;
        private readonly string _contentType;
        private readonly string _downloadName;
        private readonly ILogger _logger;
        private const int BUFFER_SIZE = 1024 * 1024; // 1 MB buffer

        public SharedFileStreamResult(string filePath, string contentType, string downloadName, ILogger logger)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _contentType = contentType ?? "application/octet-stream";
            _downloadName = downloadName ?? Path.GetFileName(filePath);
            _logger = logger;
        }

        public override void ExecuteResult(ControllerContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            var response = context.HttpContext.Response;
            var request = context.HttpContext.Request;

            try
            {
                var fileInfo = new FileInfo(_filePath);
                if (!fileInfo.Exists)
                {
                    throw new FileNotFoundException("File not found on disk.", _filePath);
                }

                long fileLength = fileInfo.Length;
                long startByte = 0;
                long endByte = fileLength - 1;
                bool isRangeRequest = false;

                // ✅ SUPPORT RANGE REQUESTS (required for large file resume/seeking)
                var rangeHeader = request.Headers["Range"];
                if (!string.IsNullOrEmpty(rangeHeader) && rangeHeader.StartsWith("bytes="))
                {
                    isRangeRequest = true;
                    var range = rangeHeader.Replace("bytes=", "").Split('-');

                    if (long.TryParse(range[0], out var parsedStart))
                        startByte = parsedStart;

                    if (range.Length > 1 && !string.IsNullOrEmpty(range[1]) && long.TryParse(range[1], out var parsedEnd))
                        endByte = parsedEnd;

                    // Validate range
                    if (startByte >= fileLength || endByte >= fileLength || startByte > endByte)
                    {
                        response.StatusCode = 416; // Requested Range Not Satisfiable
                        response.AddHeader("Content-Range", $"bytes */{fileLength}");
                        return;
                    }

                    response.StatusCode = 206; // Partial Content
                    response.AddHeader("Content-Range", $"bytes {startByte}-{endByte}/{fileLength}");

                    _logger?.Information("Range request for {FileName}: bytes {Start}-{End}/{Total}",
                        _downloadName, startByte, endByte, fileLength);
                }
                else
                {
                    response.StatusCode = 200;
                }

                long contentLength = endByte - startByte + 1;

                // Set headers
                response.ContentType = _contentType;
                response.AddHeader("Content-Disposition", $"attachment; filename=\"{_downloadName}\"");
                response.AddHeader("Content-Length", contentLength.ToString());
                response.AddHeader("Accept-Ranges", "bytes");
                response.AddHeader("Cache-Control", "private, max-age=3600"); // 1 hour cache for performance
                response.AddHeader("ETag", $"\"{fileInfo.LastWriteTimeUtc.Ticks:X}\""); // Enable browser caching

                // ✅ CRITICAL FIX: Open with FileShare.Read to allow concurrent access
                using (var fileStream = new FileStream(
                    _filePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,  // ← ALLOWS MULTIPLE READERS
                    BUFFER_SIZE,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    if (startByte > 0)
                    {
                        fileStream.Seek(startByte, SeekOrigin.Begin);
                    }

                    byte[] buffer = new byte[BUFFER_SIZE];
                    long bytesRemaining = contentLength;

                    while (bytesRemaining > 0)
                    {
                        // Check if client disconnected
                        if (!response.IsClientConnected)
                        {
                            _logger?.Information("Client disconnected during download of {FileName} at {Percent}%",
                                _downloadName,
                                (int)((contentLength - bytesRemaining) * 100.0 / contentLength));
                            break;
                        }

                        int toRead = (int)Math.Min(buffer.Length, bytesRemaining);
                        int bytesRead = fileStream.Read(buffer, 0, toRead);

                        if (bytesRead <= 0) break; // End of stream

                        response.OutputStream.Write(buffer, 0, bytesRead);
                        bytesRemaining -= bytesRead;

                        // Flush periodically to send data to client (improves large file performance)
                        if (bytesRemaining % (BUFFER_SIZE * 10) < BUFFER_SIZE)
                        {
                            response.Flush();
                        }
                    }

                    response.Flush();
                }

                _logger?.Information("Download completed successfully for {FileName} ({Size} bytes{Range})",
                    _downloadName,
                    contentLength,
                    isRangeRequest ? $" [Range: {startByte}-{endByte}]" : "");
            }
            catch (IOException ioEx) when (ioEx.Message.Contains("being used by another process"))
            {
                // This should no longer happen with FileShare.Read, but log if it does
                _logger?.Error(ioEx, "⚠️ File still locked despite FileShare.Read: {FilePath}", _filePath);
                throw new HttpException(500, "File is temporarily locked. Please try again in a few moments.", ioEx);
            }
            catch (HttpException)
            {
                // Client disconnected or other HTTP-specific error; already logged
                throw;
            }
            catch (Exception ex)
            {
                _logger?.Error(ex, "Unexpected error streaming file {FileName}", _downloadName);
                throw new HttpException(500, "An error occurred while downloading the file.", ex);
            }
        }
    }
}