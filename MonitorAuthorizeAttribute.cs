// File: Filters/MonitorAuthorizeAttribute.cs
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BOBDrive.Models;
using System.Data.Entity;

namespace BOBDrive.Filters
{
    public class MonitorAuthorizeAttribute : AuthorizeAttribute
    {
        protected override bool AuthorizeCore(HttpContextBase httpContext)
        {
            if (!base.AuthorizeCore(httpContext))
                return false;

            var username = httpContext.User.Identity.Name;
            if (string.IsNullOrWhiteSpace(username))
                return false;

            using (var db = new CloudStorageDbContext())
            {
                var user = db.Users.AsNoTracking()
                    .FirstOrDefault(u => u.ExternalUserId == username);

                if (user == null)
                    return false;

                // Must have a department and be data monitor of at least one department
                if (!user.DepartmentId.HasValue)
                    return false;

                var monitorsAny = db.Departments
                    .Any(d => d.DataMonitorUserId == user.Id);

                return monitorsAny;
            }
        }

        protected override void HandleUnauthorizedRequest(AuthorizationContext filterContext)
        {
            // If authenticated but not a monitor anymore, send to SelectDepartment
            if (filterContext.HttpContext.User.Identity.IsAuthenticated)
            {
                filterContext.Result = new RedirectToRouteResult(
                    new System.Web.Routing.RouteValueDictionary(
                        new { controller = "Account", action = "SelectDepartment" }));
            }
            else
            {
                base.HandleUnauthorizedRequest(filterContext);
            }
        }
    }
}