// File: Controllers/AccountController.cs
// REPLACE ENTIRE FILE - ALL WARNINGS FIXED
using BOBDrive.App_Start;
using BOBDrive.Hubs;
using BOBDrive.Models;
using BOBDrive.ViewModels;
using BOBDrive.ViewModels.Account;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.DirectoryServices;
using System.DirectoryServices.AccountManagement;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Helpers;
using System.Web.Mvc;
using System.Web.Security;

namespace BOBDrive.Controllers
{
    public class AccountController : BaseController
    {
        private static readonly ILogger _log = Log.ForContext<AccountController>();
        private static readonly ILogger _auditLog = LoggingConfig.AuditLogger;

        private bool AuthenticateAD(string userName, string password, string domain, out string message, out string fullName)
        {
            message = "";
            fullName = "";
            DirectoryEntry entry = new DirectoryEntry("LDAP://" + domain, userName, password);

            try
            {
                // Line 26 FIX: Use discard to avoid IDE0059 warning
                var _ = entry.NativeObject;

                DirectorySearcher search = new DirectorySearcher(entry)
                {
                    Filter = "(SAMAccountName=" + userName + ")"
                };
                search.PropertiesToLoad.Add("cn");

                SearchResult result = search.FindOne();

                if (result == null)
                {
                    message = "User account not found in Active Directory.";
                    return false;
                }

                if (result.Properties["cn"].Count > 0)
                {
                    fullName = result.Properties["cn"][0].ToString();
                }

                message = result.Path;
            }
            catch (Exception ex)
            {
                message = "Authentication failed. " + ex.Message;
                return false;
            }

            return true;
        }

        private bool IsUserInActiveDirectory(string userId)
        {
            if (string.IsNullOrWhiteSpace(userId))
            {
                return false;
            }

            try
            {
                using (var context = new PrincipalContext(ContextType.Domain, UploadConfiguration.ActiveDirectoryDomain))
                {
                    using (var userPrincipal = UserPrincipal.FindByIdentity(context, IdentityType.SamAccountName, userId))
                    {
                        return userPrincipal != null;
                    }
                }
            }
            catch (Exception ex)
            {
                _auditLog.Error(ex, "An error occurred during Active Directory validation for UserID: {UserId}", userId);
                return false;
            }
        }

        [AllowAnonymous]
        public ActionResult Login(string returnUrl, string username = "")
        {
            // If user is already authenticated, route them based on current department/approval state
            if (User.Identity.IsAuthenticated)
            {
                var externalId = User.Identity.Name;
                using (var ctx = new CloudStorageDbContext())
                {
                    var existingUser = ctx.Users.FirstOrDefault(u => u.ExternalUserId == externalId);

                    if (existingUser == null)
                    {
                        // Auth cookie exists but no local row yet -> force department selection
                        return RedirectToAction("SelectDepartment");
                    }

                    if (!existingUser.DepartmentId.HasValue)
                    {
                        return RedirectToAction("SelectDepartment");
                    }

                    if (!existingUser.IsDepartmentApproved)
                    {
                        return RedirectToAction("ApprovalPending");
                    }
                }

                // Fully approved user -> go to Home (respect picker flag in returnUrl if present)
                bool isPicker = !string.IsNullOrEmpty(returnUrl) && returnUrl.Contains("picker=true");
                return RedirectToAction("Index", "Home", new { picker = isPicker });
            }

            ViewBag.ReturnUrl = returnUrl;

            if (!string.IsNullOrEmpty(returnUrl) && returnUrl.Contains("picker=true"))
            {
                ViewBag.IsPickerLogin = true;
            }

            var model = new LoginViewModel { UserId = username };
            return View(model);
        }

        [HttpPost]
        [AllowAnonymous]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Login(LoginViewModel model, string returnUrl)
        {
            // IMPORTANT: if already authenticated, do NOT process this old login form.
            // This avoids anti-forgery mismatches when submitting a cached page.
            if (User.Identity.IsAuthenticated)
            {
                var externalId = User.Identity.Name;
                var existingUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);

                if (existingUser == null)
                {
                    return RedirectToAction("SelectDepartment");
                }

                if (!existingUser.DepartmentId.HasValue)
                {
                    return RedirectToAction("SelectDepartment");
                }

                if (!existingUser.IsDepartmentApproved)
                {
                    return RedirectToAction("ApprovalPending");
                }

                // Fully approved -> just go home
                return RedirectToAction("Index", "Home");
            }

            if (!ModelState.IsValid)
                return View(model);

            // Toggle this flag to true when AD is available in your environment.
            // Keep false for local testing so AD calls are skipped.
            bool useAd = false;

            // Prepare outputs for AD authentication (if enabled)
            string authMessage = null;
            string fullName = null;
            bool adOk = false;

            if (useAd)
            {
                // Real AD authentication (production)
                try
                {
                    adOk = AuthenticateAD(model.UserId, model.Password, UploadConfiguration.ActiveDirectoryDomain, out authMessage, out fullName);
                }
                catch (Exception ex)
                {
                    adOk = false;
                    authMessage = "AD authentication error: " + ex.Message;
                }
            }
            else
            {
                // TESTING MODE: treat AD as successful so you can exercise the rest of the app locally.
                // Do NOT forget to set useAd = true before deploying to production.
                adOk = true;
                fullName = model.UserId; // fallback display name while testing
                authMessage = "AD authentication skipped (testing mode)";
            }

            if (!adOk)
            {
                // AD auth failed -> do NOT create a local user. Show invalid credentials.
                ModelState.AddModelError("", "Invalid User ID or password.");
                _auditLog?.Warning("LOGIN_FAILED_AD: UserId={UserId}, IP={IP}, Message={Msg}", model.UserId, GetClientIpAddress(), authMessage);
                return View(model);
            }

            // AD authentication succeeded (or skipped for testing).
            // Find existing local user (may be null - we do NOT auto-create here).
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == model.UserId);

            if (user != null)
            {
                // Existing user -> existing flow
                user.LastLoginAt = DateTime.UtcNow;
                await db.SaveChangesAsync();

                // Ensure root folder exists, retry on race
                Folder userRootFolder = null;
                int retries = 0;
                const int maxRetries = 4;
                while (retries < maxRetries)
                {
                    userRootFolder = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id);
                    if (userRootFolder != null) break;

                    var folder = new Folder
                    {
                        Name = user.ExternalUserId,
                        ParentFolderId = null,
                        OwnerUserId = user.Id,
                        CreatedAt = DateTime.UtcNow
                    };
                    db.Folders.Add(folder);
                    try
                    {
                        await db.SaveChangesAsync();
                        userRootFolder = folder;
                        break;
                    }
                    catch (System.Data.Entity.Infrastructure.DbUpdateException)
                    {
                        db.Entry(folder).State = System.Data.Entity.EntityState.Detached;
                        _log?.Warning("Race condition detected while creating root folder for {UserId}, retry {Retry}", user.ExternalUserId, retries + 1);
                    }
                    retries++;
                    if (retries < maxRetries) await Task.Delay(150);
                }

                if (userRootFolder == null)
                {
                    _log?.Error("Failed to create root folder for user {UserId} after {Retries} retries", user.ExternalUserId, maxRetries);
                    ModelState.AddModelError("", "Could not find or create your personal drive folder. Please try logging out and in again.");
                    return View(model);
                }

                // Department / approval checks
                FormsAuthentication.SetAuthCookie(user.ExternalUserId, model.RememberMe);
                Session["UserFullName"] = user.FullName;

                if (!user.DepartmentId.HasValue)
                {
                    return RedirectToAction("SelectDepartment");
                }

                if (!user.IsDepartmentApproved)
                {
                    return RedirectToAction("ApprovalPending");
                }

                // Normal successful login
                TempData["SuccessMessage"] = "Login successful to BOBDrive!";

                _log?.Information("User logged in successfully: {UserId}, Role: {Role}, IP: {IP}", user.ExternalUserId, user.Role, GetClientIpAddress());
                _auditLog?.Information("USER_LOGIN: UserId={UserId}, Role={Role}, IP={IP}, Timestamp={Timestamp}",
                    user.ExternalUserId, user.Role, GetClientIpAddress(), DateTime.UtcNow);

                if (Url.IsLocalUrl(returnUrl))
                {
                    if (returnUrl.IndexOf("picker=true", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        return RedirectToAction("Index", "Home", new { folderId = userRootFolder.Id, picker = true });
                    }
                    return Redirect(returnUrl);
                }

                return RedirectToAction("Index", "Home", new { folderId = userRootFolder.Id });
            }
            else
            {
                // AD-authenticated user but no local DB row exists yet.
                // Per requirement: do NOT create DB entry now. Let the user select a department first.
                FormsAuthentication.SetAuthCookie(model.UserId, model.RememberMe);
                Session["UserFullName"] = !string.IsNullOrWhiteSpace(fullName) ? fullName : model.UserId;

                _log?.Information("AD user '{UserId}' authenticated (or AD skipped for testing) but has no local DB row - redirecting to SelectDepartment.", model.UserId);
                _auditLog?.Information("USER_AUTH_NO_LOCAL: UserId={UserId}, IP={IP}, Timestamp={Timestamp}", model.UserId, GetClientIpAddress(), DateTime.UtcNow);

                return RedirectToAction("SelectDepartment");
            }
        }




        // What this does:
        // - GET: shows department selector, handles first-run bootstrap (creates Administration dept + admin user only when DB has no departments).
        //   It does NOT create a user record on the GET (user must POST to create account).
        // - POST: creates or updates the local User record when the user explicitly selects a department.
        //   Sets IsDepartmentApproved = false so admin approval is required.
        // - Both methods are defensive: they tolerate missing DB state and produce informative ViewBag values.
        //
        // Note: This code uses your existing db context, AdminStatsHub.BroadcastApprovalsChanged(), GetUserDisplayName(), and UploadConfiguration.
        // If those symbols are named differently in your project, rename appropriately.

        [Authorize]
        public async Task<ActionResult> SelectDepartment(string returnUrl = null)
        {
            var externalId = User.Identity?.Name ?? "";

            // --- FIRST-RUN BOOTSTRAP: create Administration dept + admin user if no departments exist ---
            if (!await db.Departments.AnyAsync())
            {
                // Create or find a local user for bootstrap (so the system has a user to assign as data monitor/admin)
                var bootstrapUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
                if (bootstrapUser == null)
                {
                    bootstrapUser = new User
                    {
                        ExternalUserId = externalId,
                        Username = externalId,
                        FullName = GetUserDisplayName() ?? externalId,
                        Role = "Admin",
                        CreatedAt = DateTime.UtcNow,
                        ExternalRoleId = UploadConfiguration.DefaultUserRoleId,
                        PasswordHash = Crypto.HashPassword(Guid.NewGuid().ToString("N")),
                        IsDepartmentApproved = true,
                        IsDepartmentRejected = false
                    };
                    db.Users.Add(bootstrapUser);
                    await db.SaveChangesAsync();
                }

                var adminDept = new Department
                {
                    Name = "Administration",
                    DataMonitorUserId = bootstrapUser.Id,
                    IsVisibilityAllowed = true,
                    IsZippingAllowed = true,
                    IsDownloadingAllowed = true,
                    IsSharingAllowed = true,
                    IsExternalSharingAllowed = true,
                    IsCopyFromOtherDriveAllowed = true,
                    CreatedAt = DateTime.UtcNow,
                    AllowedFileExtensions = new List<AllowedFileExtension>()
                };

                db.Departments.Add(adminDept);
                await db.SaveChangesAsync();

                // Associate bootstrap user with admin dept and approve
                await db.Database.ExecuteSqlCommandAsync(
                    "UPDATE dbo.Users SET DepartmentId = @p0, IsDepartmentApproved = 1, IsDepartmentRejected = 0 WHERE Id = @p1",
                    adminDept.Id, bootstrapUser.Id);

                _log?.Information("System Bootstrap: Created 'Administration' department and promoted {User} to Admin.", bootstrapUser.Username);

                // redirect to home after bootstrap
                return RedirectToAction("Index", "Home");
            }

            // --- Provide helpful rejection message if any (best-effort; don't fail view on errors) ---
            try
            {
                var isRejected = await db.Database.SqlQuery<int>(
                    "SELECT CAST(ISNULL(IsDepartmentRejected, 0) AS int) FROM dbo.Users WHERE ExternalUserId = @p0",
                    externalId).FirstOrDefaultAsync();

                if (isRejected == 1)
                {
                    ViewBag.RejectionMessage = "Your department access request was rejected. Please select the department again or contact your administrator.";
                }
            }
            catch
            {
                // ignore - informational only
            }

            // If user already has an approved department, forward to Home
            var existingUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
            if (existingUser != null)
            {
                if (existingUser.DepartmentId.HasValue && existingUser.IsDepartmentApproved)
                    return RedirectToAction("Index", "Home");

                if (existingUser.DepartmentId.HasValue && !existingUser.IsDepartmentApproved)
                    return RedirectToAction("ApprovalPending");
            }

            // Build departments SelectList for the view
            var depts = await db.Departments.OrderBy(d => d.Name).ToListAsync();
            ViewBag.Departments = new SelectList(depts, "Id", "Name");
            ViewBag.ReturnUrl = returnUrl;

            return View();
        }

        [HttpPost]
        [Authorize]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> SelectDepartment(int departmentId, string returnUrl = null)
        {
            var externalId = User.Identity?.Name ?? "";
            if (string.IsNullOrWhiteSpace(externalId))
            {
                // Defensive: if somehow the user is not authenticated, send to login.
                return RedirectToAction("Login", "Account");
            }

            // Validate department exists
            var dept = await db.Departments.FindAsync(departmentId);
            if (dept == null)
            {
                ModelState.AddModelError("", "Selected department does not exist.");
                // Rebuild the dropdown and redisplay view
                var depts = await db.Departments.OrderBy(d => d.Name).ToListAsync();
                ViewBag.Departments = new SelectList(depts, "Id", "Name");
                return View();
            }

            // Create local user record now if missing (the explicit action point)
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
            if (user == null)
            {
                user = new User
                {
                    ExternalUserId = externalId,
                    Username = externalId,
                    FullName = GetUserDisplayName() ?? externalId,
                    Role = "User",
                    CreatedAt = DateTime.UtcNow,
                    ExternalRoleId = UploadConfiguration.DefaultUserRoleId,
                    PasswordHash = Crypto.HashPassword(Guid.NewGuid().ToString("N")),
                    IsDepartmentApproved = false,
                    IsDepartmentRejected = false,
                    DepartmentId = departmentId
                };
                db.Users.Add(user);
                await db.SaveChangesAsync();
            }
            else
            {
                // Update department choice and reset approval status
                user.DepartmentId = departmentId;
                user.IsDepartmentApproved = false;
                user.IsDepartmentRejected = false;
                await db.SaveChangesAsync();
            }

            // Inform any admin dashboard about pending approvals (best-effort)
            try { AdminStatsHub.BroadcastApprovalsChanged(); } catch { }

            // Send user to pending page where they are told to wait for approval
            return RedirectToAction("ApprovalPending");
        }


        // --- Helper Methods to extract info from Claims (add these to your Controller) ---

        private string GetUserDisplayName()
        {
            var identity = User.Identity as System.Security.Claims.ClaimsIdentity;
            if (identity == null) return User.Identity.Name;

            return identity.FindFirst(System.Security.Claims.ClaimTypes.Name)?.Value
                ?? identity.FindFirst("name")?.Value
                ?? User.Identity.Name;
        }

        private string GetUserEmail()
        {
            var identity = User.Identity as System.Security.Claims.ClaimsIdentity;
            if (identity == null) return null;

            return identity.FindFirst(System.Security.Claims.ClaimTypes.Email)?.Value
                ?? identity.FindFirst("email")?.Value
                ?? identity.FindFirst(System.Security.Claims.ClaimTypes.Upn)?.Value;
        }

        


        [Authorize]
        public async Task<ActionResult> ApprovalPending()
        {
            var user = await db.Users.Include(u => u.Department).FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (user.IsDepartmentApproved) return RedirectToAction("Index", "Home"); // Already approved
            if (user.DepartmentId == null) return RedirectToAction("SelectDepartment");

            return View(user.Department);
        }




        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        public ActionResult Logout(bool isPicker = false)
        {
            var username = User.Identity.Name;

            _log.Information("User logged out: {UserId}, IP: {IP}", username, GetClientIpAddress());
            _auditLog.Information("USER_LOGOUT: UserId={UserId}, IP={IP}, Timestamp={Timestamp}",
                username, GetClientIpAddress(), DateTime.UtcNow);

            FormsAuthentication.SignOut();
            Session.Clear();
            Session.Abandon();
            Response.Cookies.Add(new System.Web.HttpCookie("ASP.NET_SessionId", ""));

            TempData["SuccessMessage"] = "You have been successfully logged out from BOBDrive.";

            if (isPicker)
            {
                string returnUrl = Url.Action("Index", "Home", new { picker = true });
                return RedirectToAction("Login", "Account", new { returnUrl = returnUrl });
            }

            return RedirectToAction("Login", "Account");
        }

        // Add 'new' keyword to explicitly hide the inherited member
        private new string GetClientIpAddress()
        {
            try
            {
                string ip = Request.ServerVariables["HTTP_X_FORWARDED_FOR"];

                if (!string.IsNullOrEmpty(ip))
                {
                    string[] ipArray = ip.Split(',');
                    return ipArray[0].Trim();
                }

                ip = Request.ServerVariables["REMOTE_ADDR"];

                return !string.IsNullOrEmpty(ip) ? ip : "(unknown)";
            }
            catch
            {
                return "(unknown)";
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && db != null)
            {
                try
                {
                    db.Dispose();
                }
                catch { }
                db = null;
            }
            base.Dispose(disposing);
        }
    }
}