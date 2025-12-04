using BOBDrive.Models;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;
using System.Web.Routing;

namespace BOBDrive.Filters
{
    /// <summary>
    /// Enforces that an authenticated user:
    /// - Has a local User row
    /// - Has selected a Department (DepartmentId != null)
    /// - Has IsDepartmentApproved = true
    ///
    /// Otherwise redirects to SelectDepartment / ApprovalPending.
    /// Skip this filter for Account, AdminDashboard, MonitorDashboard, and static content.
    /// </summary>
    public class DepartmentGateAttribute : AuthorizeAttribute
    {
        // Controllers to skip (account/login, admin pages, monitor pages, etc.)
        private static readonly string[] SkipControllers = new[]
        {
            "Account",
            "AdminDashboard",
            "MonitorDashboard",
            "Error" // optional
        };

        protected override bool AuthorizeCore(System.Web.HttpContextBase httpContext)
        {
            // Let base check authentication; we only apply to authenticated users
            return base.AuthorizeCore(httpContext);
        }

        public override void OnAuthorization(AuthorizationContext filterContext)
        {
            // Let [AllowAnonymous] skip this
            if (filterContext.ActionDescriptor.IsDefined(typeof(AllowAnonymousAttribute), inherit: true) ||
                filterContext.ActionDescriptor.ControllerDescriptor.IsDefined(typeof(AllowAnonymousAttribute), inherit: true))
            {
                return;
            }

            var user = filterContext.HttpContext.User;
            if (user == null || !user.Identity.IsAuthenticated)
            {
                // Not logged in: let other auth filters handle redirect to login
                return;
            }

            var routeData = filterContext.RouteData;
            var controllerName = (routeData.Values["controller"] as string) ?? "";
            var actionName = (routeData.Values["action"] as string) ?? "";

            // Skip certain controllers entirely
            if (SkipControllers.Contains(controllerName, StringComparer.OrdinalIgnoreCase))
            {
                return;
            }

            // Also skip AccountController actions explicitly referenced (Login, SelectDepartment, ApprovalPending, Logout)
            if (controllerName.Equals("Account", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            // Now enforce department/approval
            var externalId = user.Identity.Name;
            if (string.IsNullOrWhiteSpace(externalId))
            {
                return;
            }

            using (var db = new CloudStorageDbContext())
            {
                var u = db.Users.FirstOrDefault(x => x.ExternalUserId == externalId);
                if (u == null)
                {
                    // No local user yet -> must go through SelectDepartment
                    RedirectToSelectDepartment(filterContext);
                    return;
                }

                if (!u.DepartmentId.HasValue)
                {
                    RedirectToSelectDepartment(filterContext);
                    return;
                }

                if (!u.IsDepartmentApproved)
                {
                    // Chosen a department but not yet approved
                    RedirectToApprovalPending(filterContext);
                    return;
                }
            }

            // All checks passed; allow action
        }

        private void RedirectToSelectDepartment(AuthorizationContext ctx)
        {
            ctx.Result = new RedirectToRouteResult(
                new RouteValueDictionary(new
                {
                    controller = "Account",
                    action = "SelectDepartment"
                }));
        }

        private void RedirectToApprovalPending(AuthorizationContext ctx)
        {
            ctx.Result = new RedirectToRouteResult(
                new RouteValueDictionary(new
                {
                    controller = "Account",
                    action = "ApprovalPending"
                }));
        }
    }
}