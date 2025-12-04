using BOBDrive.App_Start;
using BOBDrive.Models;
using BOBDrive.ViewModels;
using Hangfire.Logging;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.DirectoryServices.AccountManagement;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Mvc;


namespace BOBDrive.Controllers
{
    [AllowAnonymous]
    public class ShareableLinkController : BaseController
    {
        private static readonly ILogger _auditLog = LoggingConfig.AuditLogger;

        private string ComputeSha256Hash(string rawData)
        {
            using (SHA256 sha256Hash = SHA256.Create())
            {
                byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < bytes.Length; i++)
                {
                    builder.Append(bytes[i].ToString("x2"));
                }
                return builder.ToString();
            }
        }



        private bool IsUserInActiveDirectory(string userId)
        {
            if (string.IsNullOrWhiteSpace(userId)) return false;
            try
            {
                using (var context = new PrincipalContext(ContextType.Domain, UploadConfiguration.ActiveDirectoryDomain))
                using (var userPrincipal = UserPrincipal.FindByIdentity(context, IdentityType.SamAccountName, userId))
                {
                    return userPrincipal != null;
                }
            }
            catch (Exception ex)
            {
                _auditLog.Error(ex, "AD validation error for UserID: {UserId}", userId);
                return false;
            }
        }


        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<JsonResult> Create(ShareLinkCreateViewModel model)
        {
            var dept = await GetCurrentUserDepartmentAsync();

            // Per-scope gating so "external only" scenario is supported
            if (dept != null)
            {
                bool allowInternal = dept.IsSharingAllowed;
                bool allowExternal = dept.IsExternalSharingAllowed;

                if (!allowInternal && !allowExternal)
                    return Json(new { success = false, message = "Sharing is disabled for your department." });

                if (string.Equals(model.ShareScope, "Organization", StringComparison.OrdinalIgnoreCase) && !allowInternal)
                    return Json(new { success = false, message = "Internal sharing is disabled for your department." });

                if (string.Equals(model.ShareScope, "Public", StringComparison.OrdinalIgnoreCase) && !allowExternal)
                    return Json(new { success = false, message = "External sharing is disabled for your department." });
            }

            if (!ModelState.IsValid || model.FileIds == null || !model.FileIds.Any())
                return Json(new { success = false, message = "Invalid data or no files selected." });

            var files = await db.Files.Where(f => model.FileIds.Contains(f.Id) && !f.IsProcessing).ToListAsync();
            if (files.Count != model.FileIds.Count)
                return Json(new { success = false, message = "One or more selected files could not be found or are still processing." });

            var token = Guid.NewGuid().ToString("N");
            string linkPassword = null;
            var recipients = new List<ShareableLinkRecipient>();

            if (string.Equals(model.ShareScope, "Public", StringComparison.OrdinalIgnoreCase))
            {
                linkPassword = !string.IsNullOrWhiteSpace(model.Password) ? model.Password.Trim() : UploadConfiguration.GeneratePassword();

                if (!string.IsNullOrWhiteSpace(model.RecipientEmails))
                {
                    var emails = Regex.Split(model.RecipientEmails, @"[;,\r\n\t]+")
                                      .Where(e => !string.IsNullOrWhiteSpace(e) && e.Contains("@"))
                                      .Select(e => e.Trim().ToLower())
                                      .Distinct()
                                      .ToList();
                    if (!emails.Any())
                        return Json(new { success = false, message = "The email addresses provided appear to be invalid." });

                    foreach (var email in emails)
                        recipients.Add(new ShareableLinkRecipient { EmailAddress = email });
                }
            }
            else // Organization
            {
                if (model.ProtectWithPassword)
                    linkPassword = !string.IsNullOrWhiteSpace(model.Password) ? model.Password.Trim() : UploadConfiguration.GeneratePassword();

                if (!string.IsNullOrWhiteSpace(model.RecipientUserIds))
                {
                    var userIds = Regex.Split(model.RecipientUserIds, @"[;,\r\n\t]+")
                                       .Where(id => !string.IsNullOrWhiteSpace(id))
                                       .Select(id => id.Trim())
                                       .Distinct()
                                       .ToList();

                    foreach (var userId in userIds)
                    {
                        if (IsUserInActiveDirectory(userId))
                            recipients.Add(new ShareableLinkRecipient { RecipientADUserId = userId });
                        else
                            return Json(new { success = false, message = $"User ID '{userId}' does not exist in Active Directory." });
                    }
                }
            }

            DateTime? expiryDateTimeUtc = null;
            switch ((model.ExpiryOption ?? "").ToLowerInvariant())
            {
                case "30min": expiryDateTimeUtc = DateTime.UtcNow.AddMinutes(30); break;
                case "1h": expiryDateTimeUtc = DateTime.UtcNow.AddHours(1); break;
                case "12h": expiryDateTimeUtc = DateTime.UtcNow.AddHours(12); break;
                case "1d": expiryDateTimeUtc = DateTime.UtcNow.AddDays(1); break;
                case "1w": expiryDateTimeUtc = DateTime.UtcNow.AddDays(7); break;
                case "1m": expiryDateTimeUtc = DateTime.UtcNow.AddMonths(1); break;
                case "6m": expiryDateTimeUtc = DateTime.UtcNow.AddMonths(6); break;
                case "1y": expiryDateTimeUtc = DateTime.UtcNow.AddYears(1); break;
            }

            var link = new ShareableLink
            {
                Token = token,
                PasswordHash = linkPassword != null ? ComputeSha256Hash(linkPassword) : null,
                CreatedAt = DateTime.UtcNow,
                Files = files,
                ExpiresAt = expiryDateTimeUtc,
                ShareScope = model.ShareScope,
                ShareableLinkRecipients = recipients
            };

            db.ShareableLinks.Add(link);
            await db.SaveChangesAsync();

            var generatedLink = Url.Action("Access", "ShareableLink", new { token = token }, Request.Url.Scheme);
            var messageData = new
            {
                source = "BOBDriveShare",
                link = generatedLink,
                linkPassword = linkPassword,
                isMultiFile = files.Count > 1,
                files = files.Select(f => new { name = f.Name, size = f.Size, type = f.ContentType, zipPassword = f.ZipPassword }).ToList()
            };
            return Json(new { success = true, data = messageData });
        }

        [AllowAnonymous]
        [HttpGet]
        public async Task<ActionResult> Access(string token)
        {
            if (string.IsNullOrWhiteSpace(token))
                return HttpNotFound("Link token is missing.");

            var link = await db.ShareableLinks
                               .Include(l => l.Files)
                               .Include(l => l.ShareableLinkRecipients)
                               .FirstOrDefaultAsync(l => l.Token == token);

            if (link == null)
                return HttpNotFound("Invalid link.");

            var viewModel = new ShareLinkAccessViewModel
            {
                Link = link,
                Files = new List<File>(),
                IsPasswordVerified = false,
                IsExternalLoginRequired = false,
                Email = null,
                ErrorMessage = null
            };

            // Check expiry
            if (link.ExpiresAt.HasValue && link.ExpiresAt < DateTime.UtcNow)
            {
                viewModel.ErrorMessage = "This link has expired and is no longer valid.";
                _auditLog.Warning("Expired link access attempted. Token: {Token}", token);
                return View("PasswordProtect", viewModel);
            }

            // Build the visible-file list: not processing, exists on disk, and NOT soft-deleted
            var visibleFiles = link.Files
                .Where(f => !f.IsProcessing
                            && !f.IsSoftDeleted
                            && !string.IsNullOrWhiteSpace(f.FilePath)
                            && System.IO.File.Exists(f.FilePath))
                .ToList();

            if (!visibleFiles.Any())
            {
                viewModel.ErrorMessage = "No files are available for this link. They may have been deleted.";
                return View("PasswordProtect", viewModel);
            }

            // ORGANIZATION scope
            if (string.Equals(link.ShareScope, "Organization", StringComparison.OrdinalIgnoreCase))
            {
                if (!User.Identity.IsAuthenticated)
                {
                    string returnUrl = Url.Action("Access", "ShareableLink", new { token });
                    return RedirectToAction("Login", "Account", new { returnUrl });
                }

                var internalRecipients = link.ShareableLinkRecipients
                                             .Where(r => !string.IsNullOrEmpty(r.RecipientADUserId))
                                             .Select(r => r.RecipientADUserId)
                                             .ToList();

                if (internalRecipients.Any())
                {
                    string currentUserADId = User.Identity.Name;

                    if (!internalRecipients.Contains(currentUserADId, StringComparer.OrdinalIgnoreCase))
                    {
                        viewModel.ErrorMessage = "You do not have permission to access this link.";
                        _auditLog.Warning("Unauthorized access attempt to internal link. Token: {Token}, User: {User}", token, User.Identity.Name);
                        return View("PasswordProtect", viewModel);
                    }
                }

                // If password is set and not yet verified, show password form
                if (!string.IsNullOrEmpty(link.PasswordHash) && Session["VerifiedLink_" + token] == null)
                {
                    viewModel.IsPasswordVerified = false;
                    viewModel.Files = null;
                }
                else
                {
                    // Mark as shared-link access and show files
                    Session["IsSharedLinkAccess"] = true;

                    viewModel.IsPasswordVerified = true;
                    viewModel.Files = visibleFiles;
                }

                return View("PasswordProtect", viewModel);
            }

            // PUBLIC scope
            bool hasEmailRecipients = link.ShareableLinkRecipients.Any(r => !string.IsNullOrEmpty(r.EmailAddress));
            bool hasPassword = !string.IsNullOrEmpty(link.PasswordHash);

            if (hasEmailRecipients || hasPassword)
            {
                // Requires password/email through PasswordProtect POST
                viewModel.IsExternalLoginRequired = hasEmailRecipients;
                viewModel.IsPasswordVerified = false;
                viewModel.Files = null;
            }
            else
            {
                // Fully public: no password, no recipient gating
                Session["IsSharedLinkAccess"] = true;
                viewModel.IsExternalLoginRequired = false;
                viewModel.IsPasswordVerified = true;
                viewModel.Files = visibleFiles;
            }

            return View("PasswordProtect", viewModel);
        }



        [AllowAnonymous]
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> PasswordProtect(string token, string password, string email)
        {
            var viewModel = new ShareLinkAccessViewModel
            {
                IsPasswordVerified = false,
                Email = email
            };

            if (string.IsNullOrWhiteSpace(token))
            {
                viewModel.ErrorMessage = "Token cannot be empty.";
                return View("PasswordProtect", viewModel);
            }

            var link = await db.ShareableLinks
                               .Include(l => l.Files)
                               .Include(l => l.ShareableLinkRecipients)
                               .FirstOrDefaultAsync(l => l.Token == token);

            if (link == null)
                return HttpNotFound("Invalid link.");

            viewModel.Link = link;

            if (link.ExpiresAt.HasValue && link.ExpiresAt.Value < DateTime.UtcNow)
            {
                viewModel.ErrorMessage = "This link has expired and is no longer valid.";
                _auditLog.Warning("Expired link access POST. Token: {Token}, Email: {Email}", token, email);
                return View("PasswordProtect", viewModel);
            }

            if (string.IsNullOrEmpty(link.PasswordHash))
                return HttpNotFound("No password required for this link.");

            if (string.IsNullOrWhiteSpace(password))
            {
                viewModel.ErrorMessage = "Password is required.";
                return View("PasswordProtect", viewModel);
            }

            var logContext = new { Token = token, SubmittedEmail = email, RemoteIP = Request.UserHostAddress };
            var hashedPasswordAttempt = ComputeSha256Hash(password.Trim());
            if (!string.Equals(link.PasswordHash, hashedPasswordAttempt, StringComparison.OrdinalIgnoreCase))
            {
                viewModel.ErrorMessage = "Invalid password or email address.";
                _auditLog.ForContext("Details", logContext, true).Warning("Audit Failure: Invalid password.");
                return View("PasswordProtect", viewModel);
            }

            bool isAuthorized = false;

            if (string.Equals(link.ShareScope, "Organization", StringComparison.OrdinalIgnoreCase))
            {
                // For organization scope, password is sufficient (you already enforced AD recipients in GET)
                isAuthorized = true;
            }
            else // Public scope
            {
                var emailRecipients = link.ShareableLinkRecipients
                                          .Where(r => !string.IsNullOrEmpty(r.EmailAddress))
                                          .ToList();

                if (emailRecipients.Any())
                {
                    viewModel.IsExternalLoginRequired = true;

                    if (string.IsNullOrWhiteSpace(email))
                    {
                        viewModel.ErrorMessage = "Email address is required for this link.";
                        return View("PasswordProtect", viewModel);
                    }

                    var submittedEmail = email.Trim().ToLowerInvariant();
                    if (emailRecipients.Any(r => r.EmailAddress.Equals(submittedEmail, StringComparison.OrdinalIgnoreCase)))
                    {
                        isAuthorized = true;
                    }
                    else
                    {
                        viewModel.ErrorMessage = "Invalid password or email address.";
                        _auditLog.ForContext("Details", logContext, true).Warning("Audit Failure: Email not on recipient list.");
                    }
                }
                else
                {
                    // No recipient restriction, password alone is enough
                    isAuthorized = true;
                }
            }

            if (isAuthorized)
            {
                // Shared link access for downloads
                Session["IsSharedLinkAccess"] = true;

                _auditLog.ForContext("Details", logContext, true).Information("Audit Success: Access granted.");
                Session["VerifiedLink_" + token] = true;

                // Rebuild visible file list with the same filters as in GET
                var visibleFiles = link.Files
                    .Where(f => !f.IsProcessing
                                && !f.IsSoftDeleted
                                && !string.IsNullOrWhiteSpace(f.FilePath)
                                && System.IO.File.Exists(f.FilePath))
                    .ToList();

                if (!visibleFiles.Any())
                {
                    viewModel.ErrorMessage = "No files are available for this link. They may have been deleted.";
                    viewModel.Files = null;
                    viewModel.IsPasswordVerified = false;
                }
                else
                {
                    viewModel.Files = visibleFiles;
                    viewModel.IsPasswordVerified = true;
                }
            }

            return View("PasswordProtect", viewModel);
        }
    }
}