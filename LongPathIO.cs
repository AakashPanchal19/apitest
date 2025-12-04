using System;
using System.IO;

namespace BOBDrive.Services.FileOps
{
    // Long path-safe I/O helpers using \\?\ prefix when needed
    public static class LongPathIO
    {
        private const int MAX_PATH = 260;
        private const int MAX_DIR = 248;

        private static bool PathTooLong(string path)
            => !string.IsNullOrEmpty(path) && (path.Length >= MAX_PATH || (Path.GetDirectoryName(path)?.Length ?? 0) >= MAX_DIR);

        private static bool IsUnc(string fullPath) => fullPath.StartsWith(@"\\", StringComparison.Ordinal);

        private static string ToExtended(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return path;
            if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) return path;
            var full = Path.GetFullPath(path);
            if (IsUnc(full)) return @"\\?\UNC" + full.Substring(1);
            return @"\\?\" + full;
        }

        public static void EnsureDirectory(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return;

            try
            {
                if (!Directory.Exists(path)) Directory.CreateDirectory(path);
            }
            catch (PathTooLongException)
            {
                var ext = ToExtended(path);
                if (!Directory.Exists(ext)) Directory.CreateDirectory(ext);
            }
        }

        public static bool FileExists(string path)
        {
            try
            {
                if (!PathTooLong(path)) return File.Exists(path);
                return File.Exists(ToExtended(path));
            }
            catch { return false; }
        }

        public static void Copy(string source, string dest, bool overwrite)
        {
            EnsureDirectory(Path.GetDirectoryName(dest));
            try
            {
                File.Copy(source, dest, overwrite);
            }
            catch (PathTooLongException)
            {
                File.Copy(ToExtended(source), ToExtended(dest), overwrite);
            }
        }

        public static void Move(string source, string dest, bool overwrite)
        {
            EnsureDirectory(Path.GetDirectoryName(dest));
            if (overwrite && FileExists(dest))
            {
                Delete(dest);
            }

            try
            {
                File.Move(source, dest);
            }
            catch (DirectoryNotFoundException)
            {
                EnsureDirectory(Path.GetDirectoryName(dest));
                File.Move(source, dest);
            }
            catch (PathTooLongException)
            {
                var s = ToExtended(source);
                var d = ToExtended(dest);
                try { File.Move(s, d); }
                catch (IOException)
                {
                    File.Copy(s, d, overwrite);
                    try { File.Delete(s); } catch { }
                }
            }
        }

        public static void Delete(string path)
        {
            try
            {
                if (!PathTooLong(path)) { File.Delete(path); return; }
                File.Delete(ToExtended(path));
            }
            catch { }
        }
    }
}