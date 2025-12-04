namespace BOBDrive.ViewModels.Admin
{
    public class DepartmentDefaultsViewModel
    {
        public bool IsVisibilityAllowed { get; set; }
        public bool IsDownloadingAllowed { get; set; }
        public bool IsZippingAllowed { get; set; }
        public bool IsSharingAllowed { get; set; }
        public bool IsExternalSharingAllowed { get; set; }
        public bool IsCopyFromOtherDriveAllowed { get; set; }

        public int[] ExtensionIds { get; set; }
    }
}