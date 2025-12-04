using System.Collections.Generic;

namespace BOBDrive.ViewModels.Admin
{
    public class DepartmentPathRowViewModel
    {
        // Full hierarchical path: e.g. "Org A > HR > Payroll"
        public string Path { get; set; }

        // Optional Data Monitor AD User ID for the LAST node in the path
        public string DataMonitorExternalUserId { get; set; }
    }

    public class DepartmentBulkHierarchyViewModel
    {
        public List<DepartmentPathRowViewModel> Rows { get; set; }

        public DepartmentBulkHierarchyViewModel()
        {
            Rows = new List<DepartmentPathRowViewModel>();
        }
    }
}