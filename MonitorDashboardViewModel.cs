using System;
using System.Collections.Generic;

namespace BOBDrive.ViewModels
{
    /// <summary>
    /// Root view model for the Data Monitor dashboard.
    /// One user can be data monitor of at most one department.
    /// </summary>
    public class MonitorDashboardViewModel
    {
        public string MonitorUserName { get; set; }

        /// <summary>
        /// Department that this user monitors. Null if the user is not a data monitor.
        /// </summary>
        public MonitorDepartmentViewModel Department { get; set; }
    }

    public class MonitorDepartmentViewModel
    {
        public int DepartmentId { get; set; }
        public string DepartmentName { get; set; }

        // Policy flags
        public bool IsVisibilityAllowed { get; set; }
        public bool IsDownloadingAllowed { get; set; }
        public bool IsZippingAllowed { get; set; }
        public bool IsSharingAllowed { get; set; }
        public bool IsExternalSharingAllowed { get; set; }
        public bool IsCopyFromOtherDriveAllowed { get; set; }

        public List<MonitorPendingUserViewModel> PendingUsers { get; set; } =
            new List<MonitorPendingUserViewModel>();

        public List<MonitorExtensionItemViewModel> Extensions { get; set; } =
            new List<MonitorExtensionItemViewModel>();

        public List<MonitorActiveUserViewModel> ActiveUsers { get; set; } =
            new List<MonitorActiveUserViewModel>();

        // NEW: removed users and their data
        public List<MonitorRemovedUserViewModel> RemovedUsers { get; set; } =
            new List<MonitorRemovedUserViewModel>();

        public List<MonitorRemovedUserFolderViewModel> RemovedUserFolders { get; set; } =
            new List<MonitorRemovedUserFolderViewModel>();

        public List<MonitorRemovedUserFileViewModel> RemovedUserFiles { get; set; } =
            new List<MonitorRemovedUserFileViewModel>();
    }

    /// <summary>
    /// NEW: active users simplified view model for department screen
    /// </summary>
    public class MonitorActiveUserViewModel
    {
        public int UserId { get; set; }
        public string ExternalUserId { get; set; }
        public string FullName { get; set; }
        public string Role { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? LastLoginAt { get; set; }
    }

    public class MonitorRemovedUserViewModel
    {
        public int UserId { get; set; }
        public string ExternalUserId { get; set; }
        public string FullName { get; set; }
        public DateTime RemovedAt { get; set; }
    }

    public class MonitorRemovedUserFolderViewModel
    {
        public int FolderId { get; set; }
        public int OwnerUserId { get; set; }
        public string FolderName { get; set; }
        public int? ParentFolderId { get; set; }
    }

    public class MonitorRemovedUserFileViewModel
    {
        public int FileId { get; set; }
        public int FolderId { get; set; }
        public int OwnerUserId { get; set; }
        public string FileName { get; set; }
        public long Size { get; set; }
        public DateTime UploadedAt { get; set; }
    }

    public class MonitorPendingUserViewModel
    {
        public int UserId { get; set; }
        public string ExternalUserId { get; set; }
        public string FullName { get; set; }
        public DateTime CreatedAt { get; set; }
        public bool IsDepartmentApproved { get; set; }
    }

    public class MonitorExtensionItemViewModel
    {
        public int ExtensionId { get; set; }
        public string Extension { get; set; }
        public bool IsGloballyEnabled { get; set; }
        public bool IsAllowedForDepartment { get; set; }
    }

    

    /// <summary>
    /// POST model for updating department policies from monitor UI.
    /// Only contains flags that department actually has.
    /// </summary>
    public class MonitorDepartmentPolicyUpdateModel
    {
        public int DepartmentId { get; set; }
        public bool IsVisibilityAllowed { get; set; }
        public bool IsDownloadingAllowed { get; set; }
        public bool IsZippingAllowed { get; set; }
        public bool IsSharingAllowed { get; set; }
        public bool IsExternalSharingAllowed { get; set; }
        public bool IsCopyFromOtherDriveAllowed { get; set; }
    }

    /// <summary>
    /// POST model for updating allowed extensions from monitor UI.
    /// </summary>
    public class MonitorDepartmentExtensionsUpdateModel
    {
        public int DepartmentId { get; set; }

        /// <summary>
        /// IDs of AllowedFileExtension records to allow for this department.
        /// </summary>
        public int[] AllowedExtensionIds { get; set; }
    }
}