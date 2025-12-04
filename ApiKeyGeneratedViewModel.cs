using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BOBDrive.ViewModels.Admin
{
    public class ApiKeyGeneratedViewModel
    {
        public string GeneratedApiId { get; set; }
        public string ApplicationName { get; set; }
        public string BobDriveUrl { get; set; }
    }
}