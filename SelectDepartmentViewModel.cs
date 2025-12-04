using System.Collections.Generic;

namespace BOBDrive.ViewModels.Account
{
    public class SelectDepartmentItemViewModel
    {
        public int Id { get; set; }
        public string FullName { get; set; }
    }

    public class SelectDepartmentViewModel
    {
        public string RejectionMessage { get; set; }
        public List<SelectDepartmentItemViewModel> Departments { get; set; }

        public SelectDepartmentViewModel()
        {
            Departments = new List<SelectDepartmentItemViewModel>();
        }
    }
}