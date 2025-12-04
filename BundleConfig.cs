using System.Web.Optimization;

namespace BOBDrive
{
    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new ScriptBundle("~/bundles/jquery").Include(
                        "~/Scripts/jquery-{version}.js"));

            bundles.Add(new ScriptBundle("~/bundles/jqueryval").Include(
                        "~/Scripts/jquery.validate*"));

            bundles.Add(new ScriptBundle("~/bundles/modernizr").Include(
                        "~/Scripts/modernizr-*"));

            bundles.Add(new ScriptBundle("~/bundles/bootstrap").Include(
                      "~/Scripts/bootstrap.js",
                      "~/Scripts/respond.js"));

            bundles.Add(new StyleBundle("~/Content/css").Include(
                      "~/Content/bootstrap.css",
                      "~/Content/site.css"));

            // Local realtime deps: Chart.js + jQuery SignalR (no CDN)
            bundles.Add(new ScriptBundle("~/bundles/realtime").Include(
                      "~/Scripts/chart.umd.min.js",
                      "~/Scripts/jquery.signalR-2.4.3.min.js"));

            // NEW: Uppy (local, no CDN)
            bundles.Add(new ScriptBundle("~/bundles/uppy").Include(
                      "~/Scripts/uppy/uppy.min.js"));

            bundles.Add(new StyleBundle("~/Content/uppy").Include(
                      "~/Content/uppy/uppy.min.css"));
        }
    }
}