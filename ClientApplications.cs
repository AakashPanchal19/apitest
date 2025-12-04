using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace BOBDrive.Models
{
    [Table("ClientApplications")]
    public class ClientApplication
    {
        [Key]
        public int Id { get; set; }

        public Guid ApiId { get; set; }

        [Required]
        [StringLength(255)]
        public string ApplicationName { get; set; }

        [Required]
        [StringLength(500)]
        [Display(Name = "Application Base URL")]
        public string ApplicationUrl { get; set; }

        [StringLength(100)]
        [Display(Name = "Application Server IP Address (Optional)")]
        public string ServerAddress { get; set; }

        public DateTime CreatedAt { get; set; }

        public int CreatedByUserId { get; set; }

        [ForeignKey("CreatedByUserId")]
        public virtual User CreatedByUser { get; set; }
    }
}