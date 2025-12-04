using BOBDrive.Filters;
using System.Web;
using System.Web.Mvc;

namespace BOBDrive
{
    public class FilterConfig
    {
        public static void RegisterGlobalFilters(GlobalFilterCollection filters)
        {
            filters.Add(new HandleErrorAttribute());

            // Replace the old AuthorizeAttribute with our conditional wrapper
            filters.Add(new GlobalConditionalAuthorizeAttribute());

            // Replace the old DepartmentGateAttribute with our conditional wrapper
            filters.Add(new GlobalConditionalDepartmentGateAttribute());
        }
    }

    /// <summary>
    /// Global authorize filter that:
    /// - SKIPS authorization entirely for ShareableLinkController
    /// - SKIPS authorization for FileController when session indicates shared-link access
    /// - Applies normal AuthorizeAttribute behavior everywhere else
    /// </summary>
    public class GlobalConditionalAuthorizeAttribute : AuthorizeAttribute
    {
        public override void OnAuthorization(AuthorizationContext filterContext)
        {
            if (filterContext == null) return;

            var httpContext = filterContext.HttpContext;
            var routeData = httpContext.Request.RequestContext.RouteData;
            var controllerName = (routeData.Values["controller"] as string) ?? string.Empty;

            // 1) Always allow public share endpoints
            if (controllerName.Equals("ShareableLink", System.StringComparison.OrdinalIgnoreCase))
            {
                // Explicitly skip base authorization
                return;
            }

            // 2) Allow FileController actions for shared-link sessions
            if (controllerName.Equals("File", System.StringComparison.OrdinalIgnoreCase))
            {
                var isSharedLinkAccess = httpContext.Session != null &&
                                         httpContext.Session["IsSharedLinkAccess"] is bool flag &&
                                         flag;

                if (isSharedLinkAccess)
                {
                    // Skip auth if this is a download reached from a shared link
                    return;
                }
            }

            // 3) Everything else -> normal authorization
            base.OnAuthorization(filterContext);
        }
    }

    /// <summary>
    /// Global department gate filter that:
    /// - SKIPS gating entirely for ShareableLinkController
    /// - SKIPS gating for FileController when session indicates shared-link access
    /// - Applies normal DepartmentGateAttribute behavior everywhere else
    /// </summary>
    public class GlobalConditionalDepartmentGateAttribute : DepartmentGateAttribute
    {
        public override void OnAuthorization(AuthorizationContext filterContext)
        {
            if (filterContext == null) return;

            var httpContext = filterContext.HttpContext;
            var routeData = httpContext.Request.RequestContext.RouteData;
            var controllerName = (routeData.Values["controller"] as string) ?? string.Empty;

            // 1) Public share endpoints: no department checks
            if (controllerName.Equals("ShareableLink", System.StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            // 2) File downloads via shared-link: no department checks
            if (controllerName.Equals("File", System.StringComparison.OrdinalIgnoreCase))
            {
                var isSharedLinkAccess = httpContext.Session != null &&
                                         httpContext.Session["IsSharedLinkAccess"] is bool flag &&
                                         flag;

                if (isSharedLinkAccess)
                {
                    return;
                }
            }

            // 3) Everything else -> normal dept gate
            base.OnAuthorization(filterContext);
        }
    }
}