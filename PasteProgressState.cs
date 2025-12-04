namespace BOBDrive.Services.FileOps
{
    public class PasteProgressState
    {
        public string OpId { get; set; }
        public string Mode { get; set; } // "copy" or "cut"
        public int DestinationFolderId { get; set; }
        public string DestinationDisplayPath { get; set; }

        public string Stage { get; set; } = "Preparing";
        public int TotalCount { get; set; }
        public int ProcessedCount { get; set; }
        public string CurrentItemName { get; set; }

        // NEW: live byte progress
        public long TotalBytes { get; set; }
        public long ProcessedBytes { get; set; }

        public bool IsDone { get; set; }
        public string ErrorMessage { get; set; }
    }
}