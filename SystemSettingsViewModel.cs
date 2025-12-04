using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace BOBDrive.ViewModels.Admin
{
    public class SystemSettingsViewModel
    {
        // ========== Upload Configuration ==========
        [Display(Name = "Chunk Size (Bytes)")]
        public long ChunkSizeInBytes { get; set; }

        [Display(Name = "Chunk Size (MB)")]
        public double ChunkSizeInMB { get; set; }

        [Display(Name = "Max Chunk Retries")]
        public int MaxChunkRetries { get; set; }

        [Display(Name = "Checksum Sampling Threshold (Bytes)")]
        public long ChecksumSamplingThresholdBytes { get; set; }

        [Display(Name = "Checksum Sampling Threshold (MB)")]
        public double ChecksumSamplingThresholdMB { get; set; }

        [Display(Name = "Checksum Sample Size (Bytes)")]
        public int ChecksumSampleSizeBytes { get; set; }

        [Display(Name = "Checksum Sample Size (KB)")]
        public double ChecksumSampleSizeKB { get; set; }

        [Display(Name = "File Stream Buffer Size")]
        public int FileStreamBufferSize { get; set; }

        // ========== User & Authentication ==========
        [Display(Name = "Default User Role ID")]
        public int DefaultUserRoleId { get; set; }

        [Display(Name = "Active Directory Domain")]
        public string ActiveDirectoryDomain { get; set; }

        // ========== File Paths ==========
        [Display(Name = "Final Upload Path")]
        public string FinalUploadPath { get; set; }

        [Display(Name = "Temporary Chunk Path")]
        public string TempChunkPath { get; set; }

        [Display(Name = "Temporary Merge Path")]
        public string TempMergePath { get; set; }

        [Display(Name = "TUS Store Path")]
        public string TusStorePath { get; set; }

        // ========== Database Information ==========
        [Display(Name = "Database Connection String")]
        public string DatabaseConnectionString { get; set; }

        [Display(Name = "Hangfire Connection String")]
        public string HangfireConnectionString { get; set; }

        // ========== System Information ==========
        [Display(Name = "Server Name")]
        public string ServerName { get; set; }

        [Display(Name = "Application Version")]
        public string ApplicationVersion { get; set; }

        [Display(Name = "Framework Version")]
        public string FrameworkVersion { get; set; }

        [Display(Name = "Server Time (UTC)")]
        public string ServerTimeUtc { get; set; }

        [Display(Name = "Server Time (Local)")]
        public string ServerTimeLocal { get; set; }

        [Display(Name = "Application Pool")]
        public string ApplicationPool { get; set; }

        // ========== Disk Space Information ==========
        [Display(Name = "Upload Drive Total Space (GB)")]
        public double UploadDriveTotalSpaceGB { get; set; }

        [Display(Name = "Upload Drive Free Space (GB)")]
        public double UploadDriveFreeSpaceGB { get; set; }

        [Display(Name = "Upload Drive Used Space (GB)")]
        public double UploadDriveUsedSpaceGB { get; set; }

        [Display(Name = "Upload Drive Usage %")]
        public double UploadDriveUsagePercent { get; set; }

        // ========== Statistics ==========
        [Display(Name = "Total Users")]
        public int TotalUsers { get; set; }

        [Display(Name = "Total Admin Users")]
        public int TotalAdminUsers { get; set; }

        [Display(Name = "Total Files")]
        public int TotalFiles { get; set; }

        [Display(Name = "Total Storage Used (GB)")]
        public double TotalStorageUsedGB { get; set; }

        [Display(Name = "Active Upload Sessions")]
        public int ActiveUploadSessions { get; set; }

        [Display(Name = "Total Folders")]
        public int TotalFolders { get; set; }

        // ========== Hangfire Configuration ==========
        [Display(Name = "Hangfire Workers (Default Queue)")]
        public int HangfireWorkersDefault { get; set; }

        [Display(Name = "Hangfire Workers (ZIP Queue)")]
        public int HangfireWorkersZip { get; set; }

        [Display(Name = "Hangfire Workers (Maintenance Queue)")]
        public int HangfireWorkersMaintenance { get; set; }

        // ========== Allowed Extensions (NEW) ==========
        [Display(Name = "Allowed Upload Extensions (comma-separated)")]
        public string AllowedExtensionsRaw { get; set; }

        public List<string> AllowedExtensionsList { get; set; }
    }
}