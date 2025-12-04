using System.Collections.Generic;
using BOBDrive.Models;

namespace BOBDrive.ViewModels
{
    public class HomeViewModel
    {
        public Folder CurrentFolder { get; set; }
        public List<Folder> Folders { get; set; }
        public List<File> Files { get; set; }
        public List<Folder> Breadcrumbs { get; set; }

        // This was missing
        public List<Folder> AllRootFolders { get; set; }

        // This was missing (needed for "My Drive" badge logic)
        public Folder UserRootFolder { get; set; }

        public List<string> AllowedExtensions { get; set; }

        // Naming consistency: Your View uses 'IsPickerModel', Controller uses 'IsPickerMode'
        // Let's keep both for safety or rename one. Here we match the View error:
        public bool IsPickerModel { get; set; }
        // Alias for consistency if controller sets this one
        public bool IsPickerMode
        {
            get { return IsPickerModel; }
            set { IsPickerModel = value; }
        }

        // These were missing and causing errors
        public List<dynamic> OngoingUploads { get; set; }
        public List<dynamic> OngoingZippingJobs { get; set; }

        public HomeViewModel()
        {
            Folders = new List<Folder>();
            Files = new List<File>();
            Breadcrumbs = new List<Folder>();
            AllRootFolders = new List<Folder>();
            AllowedExtensions = new List<string>();
            OngoingUploads = new List<dynamic>();
            OngoingZippingJobs = new List<dynamic>();
        }
    }
}