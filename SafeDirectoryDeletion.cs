using Serilog;
using System;
using System.IO;
using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    internal static class SafeDirectoryDeletion
    {
        public static async Task<bool> TryDeleteRecursiveAsync(string dir, ILogger log, int maxAttempts = 5, int delayMs = 400)
        {
            if (string.IsNullOrWhiteSpace(dir)) return true;
            if (!Directory.Exists(dir)) return true;

            var extended = ToExtended(dir);

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    Directory.Delete(extended, true);
                    if (!Directory.Exists(extended))
                        return true;
                }
                catch (Exception ex)
                {
                    log.Warning(ex, "Directory deletion attempt {Attempt}/{Max} failed for {Dir}", attempt, maxAttempts, extended);
                }
                await Task.Delay(delayMs);
            }

            return !Directory.Exists(extended);
        }

        public static string ToExtended(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (path.StartsWith(@"\\?\")) return path;
            if (path.StartsWith(@"\\"))
                return @"\\?\UNC\" + path.Substring(2);
            return @"\\?\" + path;
        }
    }
}