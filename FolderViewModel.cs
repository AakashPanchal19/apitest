using BOBDrive.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BOBDrive.ViewModels
{
    public class FolderViewModel
    {
        // Properties for the main page view
        public List<Folder> AllRootFolders { get; set; }
        public Folder UserRootFolder { get; set; }

        // Properties for the folder contents partial view
        public Folder CurrentFolder { get; set; }
        public List<Folder> SubFolders { get; set; }
        public List<File> Files { get; set; }
        public int? ParentOfCurrentFolderId { get; set; }
        public int ChunkSize { get; set; }
        public int MaxChunkRetries { get; set; }

        // Add these new properties for sample checksum
        public long ChecksumSamplingThreshold { get; set; }
        public int ChecksumSampleSize { get; set; }
        public int ChecksumFullCalcChunkSize { get; set; }

        // Finlization prop
        public int FinalizationPoolingInterval { get; set; }

        public bool IsPickerModel { get; set; }

        // ADD THIS NEW PROPERTY
        public List<File> OngoingUploads { get; set; }

        public List<BOBDrive.Models.File> OngoingZippingJobs { get; set; }


        public List<string> AllowedExtensions { get; set; }
        public List<string> AllowedExtensionsList { get; set; }

        public FolderViewModel()
        {
            AllRootFolders = new List<Folder>();
            SubFolders = new List<Folder>();
            Files = new List<File>();

            // INITIALIZE THE NEW LIST
            OngoingUploads = new List<File>();
        }
    }
}