using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Web.Mvc;

namespace BOBDrive.ViewModels.Admin
{
    public class DepartmentCreateViewModel
    {
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        [Display(Name = "Parent Department")]
        public int? ParentDepartmentId { get; set; }

        [Display(Name = "Data Monitor (AD User ID)")]
        public string DataMonitorExternalUserId { get; set; }

        public bool IsVisibilityAllowed { get; set; }
        public bool IsZippingAllowed { get; set; }
        public bool IsDownloadingAllowed { get; set; }
        public bool IsSharingAllowed { get; set; }
        public bool IsExternalSharingAllowed { get; set; }
        public bool IsCopyFromOtherDriveAllowed { get; set; }

        public List<int> SelectedExtensionIds { get; set; }

        public MultiSelectList AvailableExtensions { get; set; }

        public SelectList ParentDepartments { get; set; }
    }
}