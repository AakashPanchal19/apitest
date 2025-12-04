using BOBDrive.Models;
using System;
using System.Data.Entity;
using System.Net;
using System.Threading.Tasks;
using System.Web.Mvc;
using System.Web.Security;

namespace BOBDrive.Controllers
{
    [AllowAnonymous]
    public class IntegrationController : BaseController
    {
        /// <summary>
        /// Secure entry point for client applications.
        /// Handles passwordless login for existing users and redirects new users to the standard login flow.
        /// </summary>
        /// <param name="username">The user's AD username. This is mandatory.</param>
        /// <param name="roleId">The user's role ID. This is optional; if not provided, the existing role is kept.</param>
        /// 
        [HttpPost]
        public async Task<ActionResult> Initiate(Guid? apiId, string username, int? roleId = null)
        {
            // **1. API ID VALIDATION (NEW LOGIC)**
            // An API ID is now mandatory for any integration attempt.
            if (!apiId.HasValue)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest, "API ID is missing.");
            }

            // Check if the provided API ID exists in our database.
            var clientApp = await db.ClientApplications.AsNoTracking()
                .FirstOrDefaultAsync(app => app.ApiId == apiId.Value);

            if (clientApp == null)
            {
                // Log this failed attempt for security monitoring if needed.
                return new HttpStatusCodeResult(HttpStatusCode.Forbidden, "Invalid or unauthorized API ID.");
            }

            // **RECOMMENDED: Referrer check for added security.**
            if (Request.UrlReferrer == null || !Request.UrlReferrer.AbsoluteUri.StartsWith(clientApp.ApplicationUrl, StringComparison.OrdinalIgnoreCase))
            {
                return new HttpStatusCodeResult(HttpStatusCode.Forbidden, "Request origin does not match registered application URL.");
            }

            // **2. ORIGINAL LOGIC (UNCHANGED)**
            // Enforce that a username must be provided.
            if (string.IsNullOrEmpty(username))
            {
                TempData["ErrorMessage"] = "Integration Failed: A username was not provided by the client application.";
                return View("Error");
            }

            // Set the security flag to prove entry was through this secure controller.
            Session["isInitiatedByClient"] = true;

            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == username);
            if (user != null)
            {
                if (roleId.HasValue && roleId.Value > 0)
                {
                    user.ExternalRoleId = roleId.Value;
                    await db.SaveChangesAsync();
                }

                FormsAuthentication.SetAuthCookie(user.ExternalUserId, createPersistentCookie: true);
                Session["UserFullName"] = user.FullName;

                return RedirectToAction("Index", "Home", new { picker = true });
            }
            else
            {
                string returnUrl = Url.Action("Index", "Home", new { picker = true });
                return RedirectToAction("Login", "Account", new { returnUrl = returnUrl, username = username });
            }
        }
    }
}