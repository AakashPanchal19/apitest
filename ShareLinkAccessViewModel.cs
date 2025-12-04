using BOBDrive.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BOBDrive.ViewModels
{
    public class ShareLinkAccessViewModel
    {
        public ShareableLink Link { get; set; }
        public string ErrorMessage { get; set; }


        public List<File> Files { get; set; }
        public bool IsPasswordVerified { get; set; }

        public string Email { get; set; }
        public bool IsExternalLoginRequired { get; set; }
    }
}