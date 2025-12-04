using System;
using System.Collections.Generic;

namespace BOBDrive.ViewModels
{
    public class MonitorArchiveViewModel
    {
        public int DepartmentId { get; set; }
        public string DepartmentName { get; set; }

        // All department monitor root folders (Dept-<id> - <name> - MonitorRoot)
        public List<FolderItem> Roots { get; set; } = new List<FolderItem>();

        // The folder being browsed
        public FolderItem CurrentFolder { get; set; }

        public List<FolderItem> ChildFolders { get; set; } = new List<FolderItem>();
        public List<FileItem> ChildFiles { get; set; } = new List<FileItem>();
    }

    public class FolderItem
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int? ParentFolderId { get; set; }
        public int? OwnerUserId { get; set; }
    }

    public class FileItem
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public long Size { get; set; }
        public DateTime UploadedAt { get; set; }
        public int FolderId { get; set; }
    }
}