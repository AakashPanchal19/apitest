using System;

namespace BOBDrive.ViewModels.Admin
{
    public class ClientApplicationViewModel
    {
        public int Id { get; set; }
        public Guid ApiId { get; set; }
        public string ApplicationName { get; set; }
        public string ApplicationUrl { get; set; }
        public string ServerAddress { get; set; }
        public DateTime CreatedAt { get; set; }
        public string CreatedByUserFullName { get; set; }
    }
}