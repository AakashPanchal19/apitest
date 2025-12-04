using BOBDrive.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Hosting;

namespace BOBDrive.App_Start
{
    /// <summary>
    /// Centralized upload configuration (DB-first compatible).
    /// Single authoritative definition (duplicate removed).
    /// </summary>
    public static class UploadConfiguration
    {
        public static readonly int ChunkSizeInBytes;
        public static readonly int MaxChunkRetries;
        public static readonly TimeSpan UploadLockTimeout;
        public static readonly int FileStreamBufferSize;

        public static readonly long ChecksumSamplingThresholdBytes;
        public static readonly int ChecksumSampleSizeBytes;
        public static readonly int ChecksumFullCalcChunkSizeBytes;

        public static readonly string ActiveDirectoryDomain;
        public static readonly int DefaultUserRoleId;
        public static readonly string SevenZipExePath;
        public static readonly double DiskSpaceCheckMultiplier;
        public static readonly int GeneratedPasswordLength;
        public static readonly string GeneratedPasswordChars;
        public static readonly int FinalizationPollingIntervalMs;

        public static readonly string FinalUploadPath;
        public static readonly string TempChunkPath;
        public static readonly string TempMergePath;
        public static readonly int MaxConcurrentZipJobs;

        // Retention (days) for incomplete sessions
        public static readonly int UploadSessionExpiryDays;

        static UploadConfiguration()
        {

            MaxConcurrentZipJobs = GetInt("MaxConcurrentZipJobs", 4);
            ChunkSizeInBytes = GetInt("ChunkSizeInMB", 10) * 1024 * 1024;
            MaxChunkRetries = GetInt("MaxChunkRetries", 3);
            UploadLockTimeout = TimeSpan.FromMinutes(GetInt("UploadLockTimeoutInMinutes", 1));
            FileStreamBufferSize = GetInt("FileStreamBufferSizeInKB", 8192) * 1024;

            long thresholdGB = GetLong("ChecksumSamplingThresholdGB", 3);
            ChecksumSamplingThresholdBytes = thresholdGB * 1024L * 1024L * 1024L;
            ChecksumSampleSizeBytes = GetInt("ChecksumSampleSizeMB", 10) * 1024 * 1024;
            ChecksumFullCalcChunkSizeBytes = GetInt("ChecksumFullCalcChunkSizeMB", 5) * 1024 * 1024;

            ActiveDirectoryDomain = GetString("ActiveDirectoryDomain", "BANKOFBARODA");
            DefaultUserRoleId = GetInt("DefaultUserRoleId", 1);
            DiskSpaceCheckMultiplier = GetDouble("DiskSpaceCheckMultiplier", 2.2);
            GeneratedPasswordLength = GetInt("GeneratedPasswordLength", 12);
            GeneratedPasswordChars = GetString("GeneratedPasswordChars", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
            FinalizationPollingIntervalMs = GetInt("FinalizationPollingIntervalMs", 2000);

            UploadSessionExpiryDays = GetInt("UploadSessionExpiryDays", 15);

            string sevenZipPathSetting = GetString("7ZipExecutablePath", "~/bin/CLI_Tools/7z.exe");
            SevenZipExePath = MapPath(sevenZipPathSetting, "7ZipExecutablePath");
            string fileUploadPathSetting = ConfigurationManager.AppSettings["FileUploadPath"];
            string tempChunkUploadPathSetting = ConfigurationManager.AppSettings["TempChunkUploadPath"];
            string tempMergePathSetting = ConfigurationManager.AppSettings["TempMergePath"];

            if (string.IsNullOrEmpty(fileUploadPathSetting))
                throw new Exception("Configuration Error: 'FileUploadPath' not set in Web.config.");
            if (string.IsNullOrEmpty(tempChunkUploadPathSetting))
                throw new Exception("Configuration Error: 'TempChunkUploadPath' not set in Web.config.");
            if (string.IsNullOrEmpty(tempMergePathSetting))
                tempMergePathSetting = tempChunkUploadPathSetting + "/MergedFiles";

            FinalUploadPath = MapPath(fileUploadPathSetting, "FileUploadPath");
            TempChunkPath = MapPath(tempChunkUploadPathSetting, "TempChunkPath");
            TempMergePath = MapPath(tempMergePathSetting, "TempMergePath");

            EnsureDir(FinalUploadPath);
            EnsureDir(TempChunkPath);
            EnsureDir(TempMergePath);
        }

        private static int GetInt(string key, int def)
            => int.TryParse(ConfigurationManager.AppSettings[key], out var v) ? v : def;
        private static long GetLong(string key, long def)
            => long.TryParse(ConfigurationManager.AppSettings[key], out var v) ? v : def;
        private static double GetDouble(string key, double def)
            => double.TryParse(ConfigurationManager.AppSettings[key], out var v) ? v : def;
        private static string GetString(string key, string def)
            => ConfigurationManager.AppSettings[key] ?? def;

        private static string MapPath(string path, string configKey)
        {
            var mapped = HostingEnvironment.MapPath(path);
            if (mapped == null)
                throw new Exception($"Configuration Error: Could not resolve path for '{configKey}'. Value was '{path}'.");
            return mapped;
        }

        private static void EnsureDir(string path)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
        }

        public static string GeneratePassword()
        {
            var validChars = GeneratedPasswordChars;
            var len = GeneratedPasswordLength;
            var sb = new System.Text.StringBuilder();
            using (var rng = new System.Security.Cryptography.RNGCryptoServiceProvider())
            {
                var buf = new byte[sizeof(uint)];
                for (int i = 0; i < len; i++)
                {
                    rng.GetBytes(buf);
                    uint num = BitConverter.ToUInt32(buf, 0);
                    sb.Append(validChars[(int)(num % (uint)validChars.Length)]);
                }
            }
            return sb.ToString();
        }

        // ===== NEW HELPERS FOR EXTENSION VALIDATION =====

        /// <summary>
        /// Normalize extension to canonical form (e.g. "pdf" -> ".pdf", upper -> lower).
        /// Returns null for empty/invalid.
        /// </summary>
        public static string NormalizeExt(string ext)
        {
            if (string.IsNullOrWhiteSpace(ext)) return null;
            ext = ext.Trim().ToLowerInvariant();
            if (!ext.StartsWith(".")) ext = "." + ext;
            return ext;
        }

        /// <summary>
        /// Reads allowed extensions from DB. 
        /// Returns null when there are no rows -> "no restriction".
        /// </summary>
        public static async Task<HashSet<string>> GetAllowedExtensionsAsync()
        {
            using (var db = new CloudStorageDbContext())
            {
                var list = await db.AllowedFileExtensions
                    .AsNoTracking()
                    .Where(e => e.IsEnabled)
                    .Select(e => e.Extension)
                    .ToListAsync();

                if (list == null || list.Count == 0)
                    return null; // null => treat as unrestricted

                return new HashSet<string>(
                    list.Select(NormalizeExt)
                        .Where(x => !string.IsNullOrWhiteSpace(x)),
                    StringComparer.OrdinalIgnoreCase
                );
            }
        }
    }
}