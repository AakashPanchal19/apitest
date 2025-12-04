using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace BOBDrive.ViewModels
{
    public class ShareLinkCreateViewModel
    {
        [Required]
        public List<int> FileIds { get; set; }
        public string FileName { get; set; } // To display in modal
        public string Password { get; set; }
        public string ExpiryOption { get; set; }
        [Required]
        public string ShareScope { get; set; } // Link Scope
        public bool ProtectWithPassword { get; set; }
        public string RecipientEmails { get; set; }
        public string RecipientUserIds { get; set; } // New property for internal user IDs
    }
}