using BOBDrive.Models;
using BOBDrive.ViewModels.Admin;
using System;
using System.Data.Entity;
using System.Net; // Required for HttpNotFound
using System.Threading.Tasks;
using System.Web.Mvc;
using BOBDrive.Filters; // ADD THIS

namespace BOBDrive.Controllers
{
    [Authorize]
    [AdminAuthorize]
    public class AdminController : BaseController
    {
        // GET: /Admin/GenerateKey
        // This method shows the initial form. It remains unchanged.
        [HttpGet]
        public ActionResult GenerateKey()
        {
            return View("GenerateKeyForm");
        }

        // POST: /Admin/GenerateKey
        // This method now only PROCESSES the form and then redirects.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> GenerateKey(ClientApplicationCreateViewModel model)
        {
            if (!ModelState.IsValid)
                return View("GenerateKeyForm", model);

            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                ModelState.AddModelError("", "Unable to identify current user.");
                return View("GenerateKeyForm", model);
            }

            var newApp = new ClientApplication
            {
                ApiId = Guid.NewGuid(),
                ApplicationName = model.ApplicationName,
                ApplicationUrl = model.ApplicationUrl,
                ServerAddress = model.ServerAddress,
                CreatedAt = DateTime.Now,
                CreatedByUserId = currentUser.Id
            };

            db.ClientApplications.Add(newApp);
            await db.SaveChangesAsync();

            // Store the success message in TempData, which survives a redirect.
            TempData["SuccessMessage"] = "Successfully generated new API Key!";

            // MODIFIED: Redirect to the new GET action, passing the new ApiId.
            // This is the "Redirect" part of the PRG pattern.
            return RedirectToAction("ApiKeyGenerated", new { id = newApp.ApiId });
        }

        // NEW GET action to display the result.
        // This is the "Get" part of the PRG pattern.
        [HttpGet]
        public async Task<ActionResult> ApiKeyGenerated(Guid id)
        {
            // Find the application that was just created.
            var app = await db.ClientApplications.AsNoTracking()
                        .FirstOrDefaultAsync(a => a.ApiId == id);

            if (app == null)
            {
                // If the ID is invalid or not found, return a 404 error.
                return new HttpStatusCodeResult(HttpStatusCode.NotFound);
            }

            // Prepare the view model to display the results.
            var viewModel = new ApiKeyGeneratedViewModel
            {
                GeneratedApiId = app.ApiId.ToString(),
                ApplicationName = app.ApplicationName,
                BobDriveUrl = Request.Url.GetLeftPart(UriPartial.Authority)
            };

            // Return the same view as before, but now in a safe, refreshable context.
            return View("ApiKeyGenerated", viewModel);
        }
    }
}