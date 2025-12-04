// File: Filters/AdminAuthorizeAttribute.cs (NEW FILE)
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BOBDrive.Models;
using System.Data.Entity;

namespace BOBDrive.Filters
{
    /// <summary>
    /// Authorization filter that restricts access to admin users only
    /// </summary>
    public class AdminAuthorizeAttribute : AuthorizeAttribute
    {
        protected override bool AuthorizeCore(HttpContextBase httpContext)
        {
            // First check if user is authenticated
            if (!base.AuthorizeCore(httpContext))
            {
                return false;
            }

            // Get the current username
            var username = httpContext.User.Identity.Name;
            if (string.IsNullOrEmpty(username))
            {
                return false;
            }

            // Check if user has admin role in database
            using (var db = new CloudStorageDbContext())
            {
                var user = db.Users.AsNoTracking()
                    .FirstOrDefault(u => u.ExternalUserId == username);

                if (user == null)
                {
                    return false;
                }

                return UserRoles.IsAdmin(user.Role);
            }
        }

        protected override void HandleUnauthorizedRequest(AuthorizationContext filterContext)
        {
            if (filterContext.HttpContext.User.Identity.IsAuthenticated)
            {
                // User is logged in but not authorized (not an admin)
                filterContext.Result = new ViewResult
                {
                    ViewName = "~/Views/Shared/AccessDenied.cshtml"
                };
            }
            else
            {
                // User is not logged in - redirect to login
                base.HandleUnauthorizedRequest(filterContext);
            }
        }
    }
}