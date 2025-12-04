// File: ViewModels/Admin/EditUserViewModel.cs (NEW FILE)
using System.ComponentModel.DataAnnotations;

namespace BOBDrive.ViewModels.Admin
{
    public class EditUserViewModel
    {
        public int Id { get; set; }

        [Display(Name = "User ID")]
        public string ExternalUserId { get; set; }

        [Required]
        [Display(Name = "Full Name")]
        [StringLength(255)]
        public string FullName { get; set; }

        [Required]
        [Display(Name = "Role")]
        public string Role { get; set; }

        [Display(Name = "External Role ID")]
        public int? ExternalRoleId { get; set; }
    }
}