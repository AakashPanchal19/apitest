using System.Web.Mvc;

namespace BOBDrive.Controllers
{
    [Authorize]
    public class TusUploadController : BaseController
    {
        // Simple page hosting Uppy Dashboard
        [HttpGet]
        public ActionResult Index()
        {
            return View();
        }
    }
}