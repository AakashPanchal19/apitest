using BOBDrive.App_Start;
using BOBDrive.Models;
using BOBDrive.ViewModels;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Mvc;
using System.Web.Security;

namespace BOBDrive.Controllers
{
    [Authorize]
    public class HomeController : BaseController
    {
        public async Task<ActionResult> Index(int? folderId, bool picker = false)
        {
            var userId = User.Identity.Name;
            var user = await db.Users
                .Include("Department")
                .Include("Department.AllowedFileExtensions")
                .FirstOrDefaultAsync(u => u.ExternalUserId == userId);

            if (user == null) return HttpNotFound("User profile not found.");

            // NEW: Always ensure user has a root drive
            var userRootFolder = await EnsureUserRootFolderAsync(user);

            Folder currentFolder = null;
            if (folderId.HasValue)
            {
                currentFolder = await db.Folders.FindAsync(folderId.Value);
                if (currentFolder == null) return HttpNotFound();
            }
            else
            {
                // If no folder explicitly requested, land the user in their own drive root
                currentFolder = userRootFolder;
                folderId = userRootFolder.Id;
            }

            var foldersQuery = db.Folders.Where(f => f.ParentFolderId == folderId);
            var filesQuery = db.Files.Where(f => f.FolderId == folderId);

            // Visibility: restrict top‑level drives if department forbids visibility
            if (!folderId.HasValue && user.Department != null && !user.Department.IsVisibilityAllowed)
            {
                foldersQuery = foldersQuery.Where(f => f.OwnerUserId == user.Id);
            }

            var folders = await foldersQuery.OrderBy(f => f.Name).ToListAsync();
            var files = await filesQuery.OrderBy(f => f.Name).ToListAsync();

            // Breadcrumbs (unchanged)
            var breadcrumbs = new List<Folder>();
            var tempFolder = currentFolder;
            while (tempFolder != null)
            {
                breadcrumbs.Insert(0, tempFolder);
                if (tempFolder.ParentFolderId.HasValue)
                    tempFolder = await db.Folders.FindAsync(tempFolder.ParentFolderId.Value);
                else
                    tempFolder = null;
            }

            // Allowed extensions (unchanged)
            List<string> allowedExtensions;
            if (user.Department != null)
            {
                if (user.Department.AllowedFileExtensions != null && user.Department.AllowedFileExtensions.Any())
                    allowedExtensions = user.Department.AllowedFileExtensions
                        .Where(e => e.IsEnabled)
                        .Select(e => e.Extension.ToLower())
                        .ToList();
                else
                    allowedExtensions = await db.AllowedFileExtensions
                        .Where(e => e.IsEnabled)
                        .Select(e => e.Extension.ToLower())
                        .ToListAsync();
            }
            else
            {
                allowedExtensions = await db.AllowedFileExtensions
                    .Where(e => e.IsEnabled)
                    .Select(e => e.Extension.ToLower())
                    .ToListAsync();
            }

            // === AllRootFolders: user drive first, then others alpha ===
            var allRootsQuery = db.Folders.Where(f => f.ParentFolderId == null);

            if (user.Department == null)
            {
                allRootsQuery = allRootsQuery.Where(f => f.OwnerUserId == user.Id);
            }
            else
            {
                if (!user.Department.IsVisibilityAllowed)
                {
                    allRootsQuery = allRootsQuery.Where(f => f.OwnerUserId == user.Id);
                }
                else
                {
                    var sameDeptApprovedUserIds = await db.Users
                        .Where(u => u.DepartmentId == user.DepartmentId
                                    && u.IsDepartmentApproved)
                        .Select(u => u.Id)
                        .ToListAsync();

                    allRootsQuery = allRootsQuery.Where(f =>
                        f.OwnerUserId.HasValue && sameDeptApprovedUserIds.Contains(f.OwnerUserId.Value));
                }
            }

            var allRootFoldersRaw = await allRootsQuery.ToListAsync();

            // Custom ordering: current user's drive always first, others alphabetically
            var allRootFolders = allRootFoldersRaw
                .OrderByDescending(f => f.OwnerUserId == user.Id) // true comes first
                .ThenBy(f => f.Name)
                .ToList();

            var viewModel = new HomeViewModel
            {
                CurrentFolder = currentFolder,
                Folders = folders,
                Files = files,
                Breadcrumbs = breadcrumbs,
                AllowedExtensions = allowedExtensions,
                IsPickerModel = picker,

                AllRootFolders = allRootFolders,
                UserRootFolder = userRootFolder,
                OngoingUploads = new List<dynamic>(),
                OngoingZippingJobs = new List<dynamic>()
            };

            ViewBag.DepartmentPolicy = user.Department;
            ViewBag.CurrentUserId = user.Id;

            return View(viewModel);
        }
    }
}