using System.Web.Mvc;
using BOBDrive.Services;
using BOBDrive.Filters;

namespace BOBDrive.Controllers
{
    [Authorize]
    [AdminAuthorize]
    public class ServerMatrixController : BaseController
    {
        public ActionResult Index()
        {
            return View();
        }

        // Fallback totals history for initial load or when SignalR fails
        [HttpGet]
        public JsonResult History(int points = 60)
        {
            var data = SystemMetricsService.GetHistory(points);
            return Json(new { data }, JsonRequestBehavior.AllowGet);
        }

        // Fallback latest processes for initial load or when SignalR fails
        [HttpGet]
        public JsonResult Processes(int max = 100)
        {
            if (max < 1) max = 1;
            if (max > 500) max = 500;
            var data = SystemMetricsService.GetLatestProcesses(max);
            return Json(new { data }, JsonRequestBehavior.AllowGet);
        }
    }
}