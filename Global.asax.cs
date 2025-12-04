using BOBDrive.Services;
using System.Web.Helpers;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;

namespace BOBDrive
{
    public class MvcApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            
            AreaRegistration.RegisterAllAreas();
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);
            BOBDrive.App_Start.LoggingConfig.Configure();

            // Do NOT enqueue Hangfire jobs here. Enqueue after storage init in OWIN startup.
            SystemMetricsService.Start(intervalMs: 1000, historyLimit: 1200);


            // IMPORTANT: prevent anti-forgery from tying the token to the previous user name ("")
            AntiForgeryConfig.SuppressIdentityHeuristicChecks = true;
            // or, alternatively:
            // AntiForgeryConfig.UniqueClaimTypeIdentifier = ClaimsIdentity.DefaultNameClaimType;
        }
    }
}