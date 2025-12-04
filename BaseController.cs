using BOBDrive.Models;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace BOBDrive.Controllers
{
    [Authorize]
    public class BaseController : Controller
    {
        protected CloudStorageDbContext db = new CloudStorageDbContext();

        /// <summary>
        /// Get the client's IP address, checking proxy headers first
        /// </summary>
        protected string GetClientIpAddress()
        {
            try
            {
                var xff = Request?.Headers?.Get("X-Forwarded-For");
                if (!string.IsNullOrWhiteSpace(xff))
                {
                    var firstIp = xff.Split(',').FirstOrDefault();
                    if (!string.IsNullOrWhiteSpace(firstIp))
                        return firstIp.Trim();
                }

                var otherHeaders = new[] { "X-Real-IP", "CF-Connecting-IP" };
                foreach (var header in otherHeaders)
                {
                    var ip = Request?.Headers?.Get(header);
                    if (!string.IsNullOrWhiteSpace(ip))
                        return ip.Trim();
                }

                return Request != null ? Request.UserHostAddress : "?.?.?.?";
            }
            catch
            {
                return "?.?.?.?";
            }
        }

        /// <summary>
        /// Safely get the current authenticated user record with Department loaded.
        /// Returns null if not authenticated or user not found.
        /// </summary>
        protected async Task<User> GetCurrentUserAsync()
        {
            try
            {
                var externalId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalId))
                    return null;

                var user = await db.Users
                    .Include(u => u.Department)
                    .FirstOrDefaultAsync(u => u.ExternalUserId == externalId);

                return user;
            }
            catch
            {
                return null;
            }
        }



        protected async Task<Folder> EnsureUserRootFolderAsync(User user)
        {
            if (user == null) return null;

            // Try to find existing root
            var existingRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id);

            if (existingRoot != null)
                return existingRoot;

            // Create new root
            var root = new Folder
            {
                Name = user.ExternalUserId,   // Drive name
                ParentFolderId = null,
                OwnerUserId = user.Id,
                CreatedAt = DateTime.UtcNow
            };
            db.Folders.Add(root);
            await db.SaveChangesAsync();
            return root;
        }



        /// <summary>
        /// Safely get the current user's Department.
        /// - Uses ViewBag.DepartmentPolicy if already set for the request.
        /// - Returns null if unauthenticated or user/department cannot be resolved.
        /// </summary>
        protected async Task<Department> GetCurrentUserDepartmentAsync()
        {
            try
            {
                // If already populated (e.g., in controller action), reuse it
                var vbDept = ViewBag.DepartmentPolicy as Department;
                if (vbDept != null)
                    return vbDept;

                var externalId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalId))
                    return null;

                // Load user with Department and allowed extensions; guard against nulls
                var user = await db.Users
                    .Include(u => u.Department)
                    .Include("Department.AllowedFileExtensions")
                    .FirstOrDefaultAsync(u => u.ExternalUserId == externalId);

                return user?.Department;
            }
            catch
            {
                // Never throw from policy helper; return null to indicate "no department"
                return null;
            }
        }

        /// <summary>
        /// Set user information in ViewBag for use in views
        /// </summary>
        protected override void OnActionExecuting(ActionExecutingContext filterContext)
        {
            // Default flags
            ViewBag.IsAdmin = false;

            if (User != null && User.Identity != null && User.Identity.IsAuthenticated)
            {
                ViewBag.CurrentUserName = User.Identity.Name;

                // Full name from session (optional)
                if (Session?["UserFullName"] != null)
                {
                    ViewBag.UserFullName = Session["UserFullName"].ToString();
                }

                // Resolve role for admin menu visibility
                try
                {
                    var username = User.Identity.Name;
                    if (!string.IsNullOrWhiteSpace(username))
                    {
                        var roleInfo = db.Users
                            .Where(u => u.ExternalUserId == username)
                            .Select(u => new { u.Role, u.Id })
                            .FirstOrDefault();

                        if (roleInfo != null)
                        {
                            ViewBag.UserRole = roleInfo.Role;
                            ViewBag.IsAdmin = UserRoles.IsAdmin(roleInfo.Role);
                            ViewBag.CurrentUserId = roleInfo.Id;
                        }
                    }
                }
                catch
                {
                    ViewBag.IsAdmin = false;
                }
            }

            base.OnActionExecuting(filterContext);
        }

        /// <summary>
        /// Safe disposal of database context
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (disposing && db != null)
            {
                try
                {
                    db.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
                finally
                {
                    db = null;
                }
            }
            base.Dispose(disposing);
        }
    }
}