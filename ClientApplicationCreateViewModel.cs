using System.ComponentModel.DataAnnotations;

namespace BOBDrive.ViewModels.Admin
{
    public class ClientApplicationCreateViewModel
    {
        [Required]
        [StringLength(255)]
        [Display(Name = "Application Name")]
        public string ApplicationName { get; set; }

        [Required]
        [StringLength(500)]
        [Url]
        [Display(Name = "Application Base URL (e.g., https://myapp.com)")]
        public string ApplicationUrl { get; set; }

        [StringLength(100)]
        [Display(Name = "Application Server IP Address (Optional)")]
        public string ServerAddress { get; set; }
    }
}