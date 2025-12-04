using BOBDrive.Filters;
using BOBDrive.Models;
using BOBDrive.ViewModels;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace BOBDrive.Controllers
{
    [Authorize]
    [MonitorAuthorize]
    public class MonitorDashboardController : BaseController
    {
        // GET: /MonitorDashboard
        public async Task<ActionResult> Index()
        {
            // Identify current user by ExternalUserId (same as in AdminDashboardController)
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            // Find the single department where this user is the data monitor
            var department = await db.Departments
                .Include(d => d.AllowedFileExtensions)
                .FirstOrDefaultAsync(d => d.DataMonitorUserId == currentUser.Id);

            var vm = new MonitorDashboardViewModel
            {
                MonitorUserName = currentUser.FullName ?? currentUser.Username ?? currentUser.ExternalUserId,
                Department = department == null ? null : await BuildDepartmentViewModelAsync(department)
            };

            return View(vm);
        }

        // GET: /MonitorDashboard/Department/{id}
        public async Task<ActionResult> Department(int id)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments
                .Include(d => d.AllowedFileExtensions)
                .FirstOrDefaultAsync(d => d.Id == id);

            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            var vm = await BuildDepartmentViewModelAsync(department);
            return View(vm);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> UpdatePolicies(MonitorDepartmentPolicyUpdateModel model)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == model.DepartmentId);
            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            // Update only policy flags; no structural changes
            department.IsVisibilityAllowed = model.IsVisibilityAllowed;
            department.IsDownloadingAllowed = model.IsDownloadingAllowed;
            department.IsZippingAllowed = model.IsZippingAllowed;
            department.IsSharingAllowed = model.IsSharingAllowed;
            department.IsExternalSharingAllowed = model.IsExternalSharingAllowed;
            department.IsCopyFromOtherDriveAllowed = model.IsCopyFromOtherDriveAllowed;

            await db.SaveChangesAsync();

            TempData["MonitorMessage"] = "Department policies updated successfully.";
            return RedirectToAction("Department", new { id = model.DepartmentId });
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> UpdateExtensions(MonitorDepartmentExtensionsUpdateModel model)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            if (model.AllowedExtensionIds == null)
            {
                model.AllowedExtensionIds = new int[0];
            }

            var department = await db.Departments
                .Include(d => d.AllowedFileExtensions)
                .FirstOrDefaultAsync(d => d.Id == model.DepartmentId);

            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            // Only extensions that are globally enabled can be used
            var globallyEnabled = await db.AllowedFileExtensions
                .Where(e => e.IsEnabled)
                .ToListAsync();

            var requestedIds = model.AllowedExtensionIds.ToList();
            var validRequested = globallyEnabled
                .Where(e => requestedIds.Contains(e.Id))
                .ToList();

            // Clear existing allowed extensions for this department
            department.AllowedFileExtensions.Clear();

            // Attach new ones
            foreach (var ext in validRequested)
            {
                // Ensure attached to context
                var existing = await db.AllowedFileExtensions.FindAsync(ext.Id);
                if (existing != null)
                {
                    department.AllowedFileExtensions.Add(existing);
                }
            }

            await db.SaveChangesAsync();

            TempData["MonitorMessage"] = "Allowed extensions updated successfully.";
            return RedirectToAction("Department", new { id = model.DepartmentId });
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> ApproveUser(int userId, int departmentId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null)
            {
                return HttpNotFound();
            }

            // Ensure this current user is actually the Data Monitor of this department
            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            var user = await db.Users.FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null)
            {
                return HttpNotFound();
            }

            // Hierarchical responsibility check: only approve if this monitor is the responsible monitor for the user
            var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(user);
            if (!responsibleMonitorId.HasValue || responsibleMonitorId.Value != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the responsible data monitor for this user.");
            }

            // At this point, the user should already have DepartmentId = departmentId
            // from their SelectDepartment / request flow.

            user.IsDepartmentApproved = true;
            user.IsDepartmentRejected = false;

            await db.SaveChangesAsync();

            // NEW: if this user currently has no root folder (OwnerUserId = user.Id, ParentFolderId = null),
            // create a brand new root folder for their active department.
            // NEW: ensure user has a fresh root folder now that they're active in this department
            var existingRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.OwnerUserId == user.Id && f.ParentFolderId == null);

            if (existingRoot == null)
            {
                await CreateNewRootFolderForUserAsync(user, department);
            }

            TempData["MonitorMessage"] = "User has been approved.";
            return RedirectToAction("Department", new { id = departmentId });
        }



        [HttpGet]
        public async Task<ActionResult> Archive(int departmentId, int? folderId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            string monitorRootName = $"Dept-{department.Id} - {department.Name} - MonitorRoot";

            var monitorRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.ParentFolderId == null &&
                                          f.OwnerUserId == null &&
                                          f.Name == monitorRootName);

            var vm = new MonitorArchiveViewModel
            {
                DepartmentId = department.Id,
                DepartmentName = department.Name
            };

            if (monitorRoot != null)
            {
                vm.Roots.Add(new FolderItem
                {
                    Id = monitorRoot.Id,
                    Name = monitorRoot.Name,
                    ParentFolderId = monitorRoot.ParentFolderId,
                    OwnerUserId = monitorRoot.OwnerUserId
                });

                // NEW: expose Bin root id to the view
                var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);
                ViewBag.BinFolderId = binFolder.Id;

                var initialFolderId = folderId ?? monitorRoot.Id;
                vm.CurrentFolder = new FolderItem
                {
                    Id = initialFolderId,
                    Name = string.Empty,
                    ParentFolderId = null
                };
            }

            return View(vm);
        }




        [HttpGet]
        public async Task<ActionResult> GetArchiveFolderContents(int departmentId, int folderId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return new HttpUnauthorizedResult();
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null || department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult();
            }

            // Try to load the folder the monitor is asking for
            var folder = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId);

            if (folder == null)
            {
                // Folder no longer exists (deleted / moved). Return a simple partial
                // instead of HttpNotFound so the AJAX call still goes through .done().
                var vmMissing = new MonitorArchiveViewModel
                {
                    DepartmentId = department.Id,
                    DepartmentName = department.Name,
                    CurrentFolder = null,
                    ChildFolders = new List<FolderItem>(),
                    ChildFiles = new List<FileItem>()
                };

                ViewBag.Breadcrumbs = new List<FolderItem>();

                return PartialView("_ArchiveFolderContents", vmMissing);
            }

            // Build breadcrumb (path from monitor root to this folder)
            var breadcrumbs = new List<FolderItem>();
            var cur = folder;
            var safety = 0;
            while (cur != null && safety++ < 100)
            {
                breadcrumbs.Add(new FolderItem
                {
                    Id = cur.Id,
                    Name = cur.Name,
                    ParentFolderId = cur.ParentFolderId,
                    OwnerUserId = cur.OwnerUserId
                });
                if (!cur.ParentFolderId.HasValue) break;
                cur = await db.Folders.FirstOrDefaultAsync(f => f.Id == cur.ParentFolderId.Value);
            }
            breadcrumbs.Reverse();

            var childFolders = await db.Folders
                .Where(f => f.ParentFolderId == folder.Id)
                .OrderBy(f => f.Name)
                .Select(f => new FolderItem
                {
                    Id = f.Id,
                    Name = f.Name,
                    ParentFolderId = f.ParentFolderId,
                    OwnerUserId = f.OwnerUserId
                })
                .ToListAsync();

            var childFiles = await db.Files
                .Where(fl => fl.FolderId == folder.Id)
                .OrderBy(fl => fl.Name)
                .Select(fl => new FileItem
                {
                    Id = fl.Id,
                    Name = fl.Name,
                    Size = fl.Size,
                    UploadedAt = fl.UploadedAt,
                    FolderId = fl.FolderId
                })
                .ToListAsync();

            var vm = new MonitorArchiveViewModel
            {
                DepartmentId = department.Id,
                DepartmentName = department.Name,
                CurrentFolder = new FolderItem
                {
                    Id = folder.Id,
                    Name = folder.Name,
                    ParentFolderId = folder.ParentFolderId,
                    OwnerUserId = folder.OwnerUserId
                },
                ChildFolders = childFolders,
                ChildFiles = childFiles
            };

            ViewBag.Breadcrumbs = breadcrumbs;

            return PartialView("_ArchiveFolderContents", vm);
        }





        public class TransferRequestModel
        {
            public string ItemType { get; set; }   // "file" or "folder"
            public int ItemId { get; set; }
            public int DepartmentId { get; set; }
            public string TargetUserExternalId { get; set; }
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> TransferItem(TransferRequestModel model)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return Json(new { success = false, message = "Not authenticated." });
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == model.DepartmentId);
            if (department == null)
            {
                return Json(new { success = false, message = "Department not found." });
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return Json(new { success = false, message = "You are not the monitor of this department." });
            }

            if (string.IsNullOrWhiteSpace(model.TargetUserExternalId))
            {
                return Json(new { success = false, message = "Target user ID is required." });
            }

            // Find receiver
            var receiver = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == model.TargetUserExternalId);
            if (receiver == null)
            {
                return Json(new { success = false, message = "User not found in database." });
            }

            if (!receiver.DepartmentId.HasValue || receiver.DepartmentId.Value != department.Id)
            {
                return Json(new { success = false, message = "Target user is not in this department." });
            }

            if (!receiver.IsDepartmentApproved)
            {
                return Json(new { success = false, message = "Target user is not approved for this department." });
            }

            // Prevent transferring the department Bin root folder
            if (string.Equals(model.ItemType, "folder", StringComparison.OrdinalIgnoreCase))
            {
                var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);
                if (model.ItemId == binFolder.Id)
                {
                    return Json(new { success = false, message = "The Bin root folder cannot be transferred." });
                }
            }

            // Find receiver's root folder (root in their own drive)
            var receiverRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == receiver.Id);
            if (receiverRoot == null)
            {
                return Json(new { success = false, message = "Target user has no root folder yet." });
            }

            if (string.Equals(model.ItemType, "file", StringComparison.OrdinalIgnoreCase))
            {
                var file = await db.Files.FirstOrDefaultAsync(f => f.Id == model.ItemId);
                if (file == null)
                {
                    return Json(new { success = false, message = "File not found." });
                }

                // Re-parent the file directly under receiver's root
                file.FolderId = receiverRoot.Id;
                await db.SaveChangesAsync();

                return Json(new { success = true, message = "File transferred to user root." });
            }
            else if (string.Equals(model.ItemType, "folder", StringComparison.OrdinalIgnoreCase))
            {
                var folder = await db.Folders.FirstOrDefaultAsync(f => f.Id == model.ItemId);
                if (folder == null)
                {
                    return Json(new { success = false, message = "Folder not found." });
                }

                // Re-parent the folder so it becomes a child of receiver's root
                folder.ParentFolderId = receiverRoot.Id;
                folder.OwnerUserId = receiver.Id; // optional: retag ownership
                await db.SaveChangesAsync();

                return Json(new { success = true, message = "Folder transferred to user root." });
            }

            return Json(new { success = false, message = "Unknown item type." });
        }




        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteItem(string itemType, int itemId, int departmentId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return Json(new { success = false, message = "Not authenticated." });
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null)
            {
                return Json(new { success = false, message = "Department not found." });
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return Json(new { success = false, message = "You are not the monitor of this department." });
            }

            try
            {
                // Get Bin folder once to check against
                var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);

                // ---------- FILE ----------
                if (string.Equals(itemType, "file", StringComparison.OrdinalIgnoreCase))
                {
                    var file = await db.Files
                        .Include(f => f.Folder)
                        .FirstOrDefaultAsync(f => f.Id == itemId);

                    if (file == null)
                    {
                        return Json(new { success = false, message = "File not found." });
                    }

                    bool inDeptBin = await IsInDepartmentBinAsync(file.Folder, department);

                    if (!inDeptBin)
                    {
                        // First delete: move into department bin (soft delete)
                        await SoftDeleteFileToDepartmentBinAsync(file, department);
                        return Json(new { success = true, message = "File moved to department bin." });
                    }
                    else
                    {
                        // Second delete FROM department bin: schedule hard delete
                        var opId = Guid.NewGuid().ToString("N");
                        BOBDrive.Controllers.FileController.BackgroundEnqueue(() =>
                            BOBDrive.Services.FileOps.DeleteJobs.HardDeleteDirect(
                                opId,
                                currentUser.ExternalUserId,
                                GetClientIpAddress(),
                                new[] { file.Id },
                                Array.Empty<int>()));

                        return Json(new { success = true, message = "File scheduled for permanent delete from department bin.", opId });
                    }
                }
                // ---------- FOLDER ----------
                else if (string.Equals(itemType, "folder", StringComparison.OrdinalIgnoreCase))
                {
                    // Block deletion of the Bin root itself
                    if (itemId == binFolder.Id)
                    {
                        return Json(new { success = false, message = "The Bin root folder cannot be deleted." });
                    }

                    var folder = await db.Folders.FirstOrDefaultAsync(f => f.Id == itemId);
                    if (folder == null)
                    {
                        return Json(new { success = false, message = "Folder not found." });
                    }

                    bool inDeptBin = await IsInDepartmentBinAsync(folder, department);

                    if (!inDeptBin)
                    {
                        // First delete: move folder subtree into department bin (soft delete)
                        await SoftDeleteFolderToDepartmentBinAsync(folder, department);
                        return Json(new { success = true, message = "Folder moved to department bin." });
                    }
                    else
                    {
                        // Already in bin (but not the bin root itself, due to check above):
                        var opId = Guid.NewGuid().ToString("N");
                        BOBDrive.Controllers.FileController.BackgroundEnqueue(() =>
                            BOBDrive.Services.FileOps.DeleteJobs.HardDeleteDirect(
                                opId,
                                currentUser.ExternalUserId,
                                GetClientIpAddress(),
                                Array.Empty<int>(),
                                new[] { folder.Id }));

                        return Json(new { success = true, message = "Folder scheduled for permanent delete from department bin.", opId });
                    }
                }
                // ---------- UNKNOWN ----------
                else
                {
                    return Json(new { success = false, message = "Unknown item type." });
                }
            }
            catch (Exception ex)
            {
                Serilog.Log.Warning(ex,
                    "Department bin delete failed: DepartmentId={DeptId}, ItemType={ItemType}, ItemId={ItemId}",
                    departmentId, itemType, itemId);

                return Json(new { success = false, message = "Department bin delete failed." });
            }
        }






        /// <summary>
        /// Create a fresh new root folder for the given user, similar to first login.
        /// Does not delete or reuse any old folders.
        /// </summary>
        private async Task<Folder> CreateNewRootFolderForUserAsync(User user, Department department)
        {
            if (user == null)
                throw new ArgumentNullException(nameof(user));

            // Root name = external user id only (e.g. RR112234)
            string rootName = user.ExternalUserId;
            var now = DateTime.UtcNow;

            var newRoot = new Folder
            {
                Name = rootName,
                OwnerUserId = user.Id,
                ParentFolderId = null,
                CreatedAt = now
                // If Folder has other required fields, set them here.
                // e.g. IsSoftDeleted = false, DepartmentId = department?.Id, etc.
            };

            db.Folders.Add(newRoot);
            await db.SaveChangesAsync();

            return newRoot;
        }



        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> RejectUser(int userId, int departmentId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            var user = await db.Users.FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null)
            {
                return HttpNotFound();
            }

            // Hierarchical responsibility check: only reject if this monitor is the responsible monitor for the user
            var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(user);
            if (!responsibleMonitorId.HasValue || responsibleMonitorId.Value != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the responsible data monitor for this user.");
            }

            user.IsDepartmentApproved = false;
            user.IsDepartmentRejected = true;
            user.DepartmentId = null;

            await db.SaveChangesAsync();

            TempData["MonitorMessage"] = "User has been rejected and unassigned from this department.";
            return RedirectToAction("Department", new { id = departmentId });
        }



        private async Task MoveUserRootFolderToUserArchiveAsync(User user, Department department)
        {
            if (user == null) throw new ArgumentNullException(nameof(user));
            if (department == null) throw new ArgumentNullException(nameof(department));

            // Find user's root folder (owner + ParentFolderId == null)
            var userRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.OwnerUserId == user.Id && f.ParentFolderId == null);

            if (userRoot == null) return;

            // Ensure department monitor root exists
            var monitorRoot = await GetOrCreateDepartmentMonitorRootFolderAsync(department);

            // Create or get a per-user archive folder under monitor root
            var archiveName = $"User-{user.ExternalUserId} - Archive";
            var archiveFolder = await db.Folders.FirstOrDefaultAsync(f =>
                f.ParentFolderId == monitorRoot.Id &&
                f.OwnerUserId == null &&
                f.Name == archiveName);

            if (archiveFolder == null)
            {
                archiveFolder = new Folder
                {
                    Name = archiveName,
                    ParentFolderId = monitorRoot.Id,
                    OwnerUserId = null,
                    CreatedAt = DateTime.UtcNow
                };
                db.Folders.Add(archiveFolder);
                await db.SaveChangesAsync();
            }

            // Move the user's root under the per-user archive folder
            userRoot.ParentFolderId = archiveFolder.Id;

            await db.SaveChangesAsync();
        }



        /// <summary>
        /// Remove an already-approved active user from this department.
        /// - Moves the user's root folder under the department's monitor root folder
        /// - Sets DepartmentId = null, IsDepartmentApproved = false, IsDepartmentRejected = true
        /// </summary>
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> RemoveUserFromDepartment(int userId, int departmentId)
        {
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var department = await db.Departments.FirstOrDefaultAsync(d => d.Id == departmentId);
            if (department == null)
            {
                return HttpNotFound();
            }

            if (department.DataMonitorUserId != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the data monitor of this department.");
            }

            var user = await db.Users.FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null)
            {
                return HttpNotFound();
            }

            // Ensure this monitor is responsible (same logic as approve/reject)
            var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(user);
            if (!responsibleMonitorId.HasValue || responsibleMonitorId.Value != currentUser.Id)
            {
                return new HttpUnauthorizedResult("You are not the responsible data monitor for this user.");
            }

            var oldDeptId = user.DepartmentId;
            var oldDeptName = department.Name;
            var ip = GetClientIpAddress();

            
            // 1) move user's existing root under a per-user archive node for this department
            await MoveUserRootFolderToUserArchiveAsync(user, department);

            // 2) create a fresh root for user's future department usage
            await CreateNewRootFolderForUserAsync(user, department);

            // 2. Remove the user from the department (like reject).
            user.DepartmentId = null;
            user.IsDepartmentApproved = false;
            user.IsDepartmentRejected = true;

            //  clear monitor assignments for this user across departments
            var monitoredDepts = await db.Departments
                .Where(d => d.DataMonitorUserId == user.Id)
                .ToListAsync();

            foreach (var d in monitoredDepts)
            {
                d.DataMonitorUserId = null;
            }

            await db.SaveChangesAsync();

            Serilog.Log.Information(
                "DATA_MONITOR_REMOVE_USER: Monitor={MonitorExtId}, Target={TargetExtId}, OldDeptId={DeptId}, OldDeptName={DeptName}, IP={IP}, Timestamp={Ts}",
                currentUser.ExternalUserId,
                user.ExternalUserId,
                oldDeptId,
                oldDeptName,
                ip,
                DateTime.UtcNow
            );

            // If you later wire SignalR, call your hub here to force logout.
            // e.g.: UserSessionHub.ForceLogoutUser(user.ExternalUserId, "...");

            TempData["MonitorMessage"] =
                "User has been removed from this department. Their existing data is now under the department monitor root. " +
                "If they are assigned to a department again, a new root folder will be created.";

            return RedirectToAction("Department", new { id = departmentId });
        }

        /// <summary>
        /// Given a user with DepartmentId, walk up the department tree and find the nearest
        /// ancestor (including its own department) that has DataMonitorUserId assigned.
        /// Returns that monitor's UserId, or null if none found.
        /// </summary>
        private async Task<int?> GetResponsibleMonitorUserIdForUserAsync(User user)
        {
            if (user == null || !user.DepartmentId.HasValue)
                return null;

            var allDepts = await db.Departments.ToListAsync();
            var dict = allDepts.ToDictionary(d => d.Id);

            Department current;
            if (!dict.TryGetValue(user.DepartmentId.Value, out current))
                return null;

            var safety = 0;
            while (current != null && safety++ < 100)
            {
                if (current.DataMonitorUserId.HasValue)
                    return current.DataMonitorUserId.Value;

                if (!current.ParentDepartmentId.HasValue)
                    break;

                if (!dict.TryGetValue(current.ParentDepartmentId.Value, out current))
                    break;
            }

            return null;
        }



        private async Task<MonitorDepartmentViewModel> BuildDepartmentViewModelAsync(Department department)
        {
            var vm = new MonitorDepartmentViewModel
            {
                DepartmentId = department.Id,
                DepartmentName = department.Name,
                IsVisibilityAllowed = department.IsVisibilityAllowed,
                IsDownloadingAllowed = department.IsDownloadingAllowed,
                IsZippingAllowed = department.IsZippingAllowed,
                IsSharingAllowed = department.IsSharingAllowed,
                IsExternalSharingAllowed = department.IsExternalSharingAllowed,
                IsCopyFromOtherDriveAllowed = department.IsCopyFromOtherDriveAllowed
            };

            // Identify current monitor user
            var currentUser = await db.Users
                .FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);

            // ---------- Pending users (your existing logic) ----------
            var pendingUsersQuery = db.Users
                .Where(u => u.DepartmentId != null && !u.IsDepartmentApproved)
                .OrderBy(u => u.CreatedAt);

            var pendingUsersList = await pendingUsersQuery.ToListAsync();

            var monitorPendingUsers = new List<MonitorPendingUserViewModel>();

            foreach (var u in pendingUsersList)
            {
                var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(u);

                if (responsibleMonitorId.HasValue &&
                    currentUser != null &&
                    responsibleMonitorId.Value == currentUser.Id)
                {
                    monitorPendingUsers.Add(new MonitorPendingUserViewModel
                    {
                        UserId = u.Id,
                        ExternalUserId = u.ExternalUserId,
                        FullName = u.FullName,
                        CreatedAt = u.CreatedAt,
                        IsDepartmentApproved = u.IsDepartmentApproved
                    });
                }
            }

            vm.PendingUsers = monitorPendingUsers;

            // ---------- Extensions (your existing logic) ----------
            var allExts = await db.AllowedFileExtensions
                .OrderBy(e => e.Extension)
                .ToListAsync();

            var deptExtIds = department.AllowedFileExtensions?
                .Select(e => e.Id)
                .ToList()
                ?? new List<int>();

            vm.Extensions = allExts
                .Select(e => new MonitorExtensionItemViewModel
                {
                    ExtensionId = e.Id,
                    Extension = e.Extension,
                    IsGloballyEnabled = e.IsEnabled,
                    IsAllowedForDepartment = e.IsEnabled && deptExtIds.Contains(e.Id)
                })
                .ToList();

            // ---------- Active approved users in this department ----------
            var activeUsers = await db.Users
                .Where(u => u.DepartmentId == department.Id && u.IsDepartmentApproved)
                .OrderBy(u => u.FullName)
                .ToListAsync();

            vm.ActiveUsers = activeUsers
                .Select(u => new MonitorActiveUserViewModel
                {
                    UserId = u.Id,
                    ExternalUserId = u.ExternalUserId,
                    FullName = u.FullName,
                    Role = u.Role,
                    CreatedAt = u.CreatedAt,
                    LastLoginAt = u.LastLoginAt
                })
                .ToList();

            // ---------- NEW: Removed users this monitor is responsible for ----------
            vm.RemovedUsers = new List<MonitorRemovedUserViewModel>();
            vm.RemovedUserFolders = new List<MonitorRemovedUserFolderViewModel>();
            vm.RemovedUserFiles = new List<MonitorRemovedUserFileViewModel>();

            if (currentUser == null)
                return vm;

            // We consider "removed" as DepartmentId == null && IsDepartmentRejected == true
            var removedUsersRaw = await db.Users
                .Where(u => u.DepartmentId == null && u.IsDepartmentRejected)
                .OrderByDescending(u => u.CreatedAt)
                .ToListAsync();

            var removedUserIds = new List<int>();

            foreach (var u in removedUsersRaw)
            {
                var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(u);
                if (responsibleMonitorId.HasValue && responsibleMonitorId.Value == currentUser.Id)
                {
                    vm.RemovedUsers.Add(new MonitorRemovedUserViewModel
                    {
                        UserId = u.Id,
                        ExternalUserId = u.ExternalUserId,
                        FullName = u.FullName,
                        // If you later track an explicit Remove timestamp, use that instead of CreatedAt
                        RemovedAt = u.CreatedAt
                    });
                    removedUserIds.Add(u.Id);
                }
            }

            if (!removedUserIds.Any())
                return vm;

            // ---------- Folders owned by removed users ----------
            // Adjust property names if different in your EF model
            var removedFolders = await db.Folders
                .Where(f => f.OwnerUserId.HasValue && removedUserIds.Contains(f.OwnerUserId.Value))
                .ToListAsync();

            vm.RemovedUserFolders = removedFolders
                .Select(f => new MonitorRemovedUserFolderViewModel
                {
                    FolderId = f.Id,
                    OwnerUserId = f.OwnerUserId.Value,
                    FolderName = f.Name,
                    ParentFolderId = f.ParentFolderId
                })
                .ToList();

            var folderIds = removedFolders.Select(f => f.Id).ToList();

            if (!folderIds.Any())
                return vm;

            // ---------- Files in those folders ----------
            var removedFiles = await db.Files
                .Where(file => folderIds.Contains(file.FolderId))
                .ToListAsync();

            vm.RemovedUserFiles = removedFiles
                .Select(file => new MonitorRemovedUserFileViewModel
                {
                    FileId = file.Id,
                    FolderId = file.FolderId,
                    OwnerUserId = file.Folder.OwnerUserId ?? 0,
                    FileName = file.Name,
                    Size = file.Size,
                    UploadedAt = file.UploadedAt
                })
                .ToList();

            return vm;
        }


        /// <summary>
        /// Find or create the department-level monitor root folder for the given department.
        /// This folder is shared for all removed users of that department and does not change
        /// even if the Data Monitor user changes.
        /// </summary>
        private async Task<Folder> GetOrCreateDepartmentMonitorRootFolderAsync(Department department)
        {
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            string monitorRootName = $"Dept-{department.Id} - {department.Name} - MonitorRoot";

            var existing = await db.Folders
                .FirstOrDefaultAsync(f =>
                    f.ParentFolderId == null &&
                    f.OwnerUserId == null &&
                    f.Name == monitorRootName);

            if (existing != null)
                return existing;

            var now = DateTime.UtcNow; // or DateTime.Now, but UtcNow is usually better

            var newFolder = new Folder
            {
                Name = monitorRootName,
                OwnerUserId = null,
                ParentFolderId = null,

                // IMPORTANT: initialize required datetime fields:
                CreatedAt = now,          // adjust name
                                          // If you have others:
                                          // UpdatedAt = now,
                                          // LastAccessedAt = now,
                                          // etc.

                // If you have department-specific fields:
                // DepartmentId = department.Id
            };

            db.Folders.Add(newFolder);
            await db.SaveChangesAsync();

            return newFolder;
        }




        /// <summary>
        /// Find or create the department-level Bin folder under the monitor root.
        /// This is a common Bin for all archive deletes in this department.
        /// </summary>
        private async Task<Folder> GetOrCreateDepartmentBinFolderAsync(Department department)
        {
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            // Ensure monitor root exists first
            var monitorRoot = await GetOrCreateDepartmentMonitorRootFolderAsync(department);

            // Try to find an existing "Bin" child under monitor root
            var existingBin = await db.Folders
                .FirstOrDefaultAsync(f =>
                    f.ParentFolderId == monitorRoot.Id &&
                    f.OwnerUserId == null &&
                    f.Name == "Bin");

            if (existingBin != null)
                return existingBin;

            var now = DateTime.UtcNow;

            var binFolder = new Folder
            {
                Name = "Bin",
                ParentFolderId = monitorRoot.Id,
                OwnerUserId = null,
                CreatedAt = now
                // Set any other required fields (IsSoftDeleted = false, etc.) if your schema needs them.
            };

            db.Folders.Add(binFolder);
            await db.SaveChangesAsync();

            return binFolder;
        }



        private async Task SoftDeleteFileToDepartmentBinAsync(File file, Department department)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (department == null) throw new ArgumentNullException(nameof(department));

            var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);

            // Move the file into department bin
            file.FolderId = binFolder.Id;

            // Mark as soft deleted in a consistent way so cleanup jobs can act on it
            if (!file.IsSoftDeleted)
            {
                var now = DateTime.UtcNow;
                file.IsSoftDeleted = true;
                file.SoftDeletedAt = now;
                file.BinEnteredAt = now;
            }
            file.FinalizationStage = "SoftDeleted";

            await db.SaveChangesAsync();
        }



        /// <summary>
        /// Return the set of folder IDs in the subtree rooted at the given folderId (including the root).
        /// Uses a simple BFS over Folders.ParentFolderId.
        /// </summary>
        private async Task<HashSet<int>> GetFolderSubtreeIdsForDepartmentAsync(int rootFolderId)
        {
            var result = new HashSet<int> { rootFolderId };
            var queue = new Queue<int>();
            queue.Enqueue(rootFolderId);

            while (queue.Count > 0)
            {
                var id = queue.Dequeue();

                var children = await db.Folders
                    .Where(f => f.ParentFolderId == id)
                    .Select(f => f.Id)
                    .ToListAsync();

                foreach (var childId in children)
                {
                    if (result.Add(childId))
                    {
                        queue.Enqueue(childId);
                    }
                }
            }

            return result;
        }



        /// <summary>
        /// Move the given folder (and all its children) under the department Bin folder.
        /// Also marks all files in the subtree as soft-deleted, with BinEnteredAt set,
        /// so that BinRetention jobs can later purge them.
        /// </summary>
        private async Task SoftDeleteFolderToDepartmentBinAsync(Folder folder, Department department)
        {
            if (folder == null)
                throw new ArgumentNullException(nameof(folder));
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            // 1) Ensure department Bin folder exists (under the monitor root)
            var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);

            // 2) Compute the subtree of this folder (including itself)
            // If you don't have a shared helper yet, you can inline a small BFS; here we assume one exists.
            var subtreeFolderIds = await GetFolderSubtreeIdsForDepartmentAsync(folder.Id);

            // 3) Mark all files in this subtree as soft-deleted & tagged with BinEnteredAt
            var filesInSubtree = await db.Files
                .Where(f => subtreeFolderIds.Contains(f.FolderId))
                .ToListAsync();

            var now = DateTime.UtcNow;
            foreach (var f in filesInSubtree)
            {
                if (!f.IsSoftDeleted)
                {
                    f.IsSoftDeleted = true;
                    f.SoftDeletedAt = now;
                    f.BinEnteredAt = now;
                }
                f.FinalizationStage = "SoftDeleted";
            }

            // 4) Move the root folder of this subtree under the department Bin
            folder.ParentFolderId = binFolder.Id;

            await db.SaveChangesAsync();
        }



        /// <summary>
        /// Move the given user's root folder (if any) under the given department monitor root folder.
        /// After this, the user's former root is no longer root; ParentFolderId points to monitorRoot.Id.
        /// If the user has no root folder, nothing happens.
        /// </summary>
        private async Task MoveUserRootFolderToMonitorRootAsync(User user, Department department)
        {
            if (user == null)
                throw new ArgumentNullException(nameof(user));
            if (department == null)
                throw new ArgumentNullException(nameof(department));

            // Identify user's "root" folder: owned by user, ParentFolderId == null
            var userRoot = await db.Folders
                .FirstOrDefaultAsync(f => f.OwnerUserId == user.Id && f.ParentFolderId == null);

            if (userRoot == null)
            {
                // User has no root folder yet; nothing to move.
                return;
            }

            // Get/create the department monitor root folder
            var monitorRoot = await GetOrCreateDepartmentMonitorRootFolderAsync(department);

            // Move user root under the monitor root
            userRoot.ParentFolderId = monitorRoot.Id;

            // Optionally, you may want to:
            // - Clear userRoot.OwnerUserId (so it's clear it's no longer the user's active root),
            //   or leave it to track original owner. I'll leave it to preserve provenance.
            // - Set some flag or rename it.

            await db.SaveChangesAsync();
        }



        /// <summary>
        /// Returns true if the given folder is the department bin itself
        /// or is inside the subtree of the department bin for this department.
        /// </summary>
        private async Task<bool> IsInDepartmentBinAsync(Folder folder, Department department)
        {
            if (folder == null || department == null) return false;

            var binFolder = await GetOrCreateDepartmentBinFolderAsync(department);
            if (folder.Id == binFolder.Id) return true;

            // Walk up parents until root or binFolder is found
            var safety = 0;
            var current = folder;
            while (current.ParentFolderId.HasValue && safety++ < 100)
            {
                if (current.ParentFolderId.Value == binFolder.Id)
                    return true;

                current = await db.Folders.FirstOrDefaultAsync(f => f.Id == current.ParentFolderId.Value);
                if (current == null) break;
            }

            return false;
        }



        private async Task HardDeleteDepartmentFileAsync(File file)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));

            // Resolve physical path
            var physicalPath = file.FilePath;
            if (!string.IsNullOrWhiteSpace(physicalPath) &&
                !System.IO.Path.IsPathRooted(physicalPath) &&
                (physicalPath.StartsWith("~") || physicalPath.StartsWith("/")))
            {
                try
                {
                    physicalPath = System.Web.Hosting.HostingEnvironment.MapPath(physicalPath);
                }
                catch
                {
                    // ignore mapping errors, best-effort
                }
            }

            // Check if this is last reference to that blob
            if (!string.IsNullOrWhiteSpace(physicalPath))
            {
                var refCount = await db.Files
                    .AsNoTracking()
                    .CountAsync(other => other.FilePath == file.FilePath && other.Id != file.Id);

                if (refCount == 0 && BOBDrive.Services.FileOps.LongPathIO.FileExists(physicalPath))
                {
                    try
                    {
                        BOBDrive.Services.FileOps.LongPathIO.Delete(physicalPath);
                    }
                    catch
                    {
                        // best-effort; if delete fails, we still remove DB row
                    }
                }
            }

            db.Files.Remove(file);
            await db.SaveChangesAsync();
        }



        private async Task HardDeleteDepartmentFolderSubtreeAsync(int folderId)
        {
            // Get direct children first
            var childFolders = await db.Folders
                .Where(f => f.ParentFolderId == folderId)
                .Select(f => f.Id)
                .ToListAsync();

            // Recurse into children
            foreach (var childId in childFolders)
            {
                await HardDeleteDepartmentFolderSubtreeAsync(childId);
            }

            // Delete all files in this folder
            var files = await db.Files
                .Where(f => f.FolderId == folderId)
                .ToListAsync();

            foreach (var file in files)
            {
                await HardDeleteDepartmentFileAsync(file);
                // HardDeleteDepartmentFileAsync already calls SaveChanges, which is a bit chatty
                // but safe; if you want, you can optimize by batching, but it's not required.
            }

            // Finally delete the folder itself
            var folder = await db.Folders.FirstOrDefaultAsync(f => f.Id == folderId);
            if (folder != null)
            {
                db.Folders.Remove(folder);
                await db.SaveChangesAsync();
            }
        }

    }
}