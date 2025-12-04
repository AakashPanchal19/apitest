using System.Collections.Generic;
using BOBDrive.Models;

namespace BOBDrive.ViewModels.Admin
{
    public class AdminDashboardViewModel
    {
        public int TotalUsers { get; set; }
        public int TotalAdmins { get; set; }
        public int TotalFiles { get; set; }
        public int TotalFolders { get; set; }
        public long TotalStorage { get; set; }

        /// <summary>
        /// Number of file records currently processing (uploads + zips, fallback).
        /// </summary>
        public int ActiveUploads { get; set; }

        /// <summary>
        /// Number of active ZIP jobs (ContentType == application/zip and IsProcessing = true).
        /// </summary>
        public int ActiveZipJobs { get; set; }

        /// <summary>
        /// Completed uploads in the last hour.
        /// </summary>
        public int UploadsLastHour { get; set; }

        /// <summary>
        /// Completed zips in the last hour.
        /// </summary>
        public int ZipsLastHour { get; set; }

        public int TotalClientApps { get; set; }
        public List<User> RecentUsers { get; set; }
    }
}