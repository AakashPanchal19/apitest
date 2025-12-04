using BOBDrive.App_Start;
using BOBDrive.Filters;
using BOBDrive.Hubs;
using BOBDrive.Infrastructure;
using BOBDrive.Models;
using BOBDrive.ViewModels.Admin;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Web.Hosting;
using System.Web.Mvc;

namespace BOBDrive.Controllers
{
    [Authorize]
    [AdminAuthorize]
    public class AdminDashboardController : BaseController
    {
        // DTO for hierarchy JSON sent from the hierarchy builder
        public class HierarchyNodeDto
        {
            public string name { get; set; }
            public string path { get; set; }
            public List<HierarchyNodeDto> children { get; set; }
        }

        // GET: /AdminDashboard
        public async Task<ActionResult> Index()
        {
            var oneHourAgo = DateTime.UtcNow.AddHours(-1);
            var now = DateTime.UtcNow;
            var cutoff = now.AddMinutes(-1); // "currently active" window

            var activeUploads = await (
                from s in db.UploadSessions
                where s.Status == 0
                      && s.UpdatedAt >= cutoff
                join f in db.Files
                    on s.TusFileId equals f.FileIdForUpload into fileGroup
                from f in fileGroup.DefaultIfEmpty()
                where f == null || f.FilePath == null || f.FilePath == ""
                select s
            ).CountAsync();

            var stats = new AdminDashboardViewModel
            {
                TotalUsers = await db.Users.CountAsync(),
                TotalAdmins = await db.Users.CountAsync(u => u.Role == UserRoles.Admin),
                TotalFiles = await db.Files.CountAsync(f => !f.IsProcessing),
                TotalFolders = await db.Folders.CountAsync(),
                TotalStorage = await db.Files
                    .Where(f => !f.IsProcessing)
                    .SumAsync(f => (long?)f.Size) ?? 0,
                ActiveUploads = activeUploads,
                ActiveZipJobs = await db.Files
                    .CountAsync(f => f.IsProcessing && f.ContentType == "application/zip"),
                UploadsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType != "application/zip"
                                     && f.UploadedAt >= oneHourAgo),
                ZipsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType == "application/zip"
                                     && f.UploadedAt >= oneHourAgo),
                TotalClientApps = await db.ClientApplications.CountAsync(),
                RecentUsers = await db.Users
                    .OrderByDescending(u => u.LastLoginAt)
                    .Take(10)
                    .ToListAsync()
            };

            return View(stats);
        }



        [HttpGet]
        [Authorize]
        [AdminAuthorize]
        public async Task<JsonResult> IsUserRegistered(string externalUserId)
        {
            if (string.IsNullOrWhiteSpace(externalUserId))
            {
                return Json(new { exists = false }, JsonRequestBehavior.AllowGet);
            }

            var id = externalUserId.Trim();
            bool exists = false;
            try
            {
                exists = await db.Users.AnyAsync(u => u.ExternalUserId == id);
            }
            catch
            {
                // best-effort: on DB error, return false so client-side shows error
                exists = false;
            }

            return Json(new { exists = exists }, JsonRequestBehavior.AllowGet);
        }




        // GET: /AdminDashboard/Users
        public async Task<ActionResult> Users()
        {
            var users = await db.Users
                .OrderBy(u => u.FullName)
                .ToListAsync();

            return View(users);
        }

        // GET: /AdminDashboard/EditUser/5
        public async Task<ActionResult> EditUser(int id)
        {
            var user = await db.Users.FindAsync(id);
            if (user == null)
            {
                return HttpNotFound();
            }

            var viewModel = new EditUserViewModel
            {
                Id = user.Id,
                ExternalUserId = user.ExternalUserId,
                FullName = user.FullName,
                Role = user.Role,
                ExternalRoleId = user.ExternalRoleId
            };

            return View(viewModel);
        }

        // POST: /AdminDashboard/EditUser
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> EditUser(EditUserViewModel model)
        {
            if (!ModelState.IsValid)
            {
                return View(model);
            }

            var user = await db.Users.FindAsync(model.Id);
            if (user == null)
            {
                return HttpNotFound();
            }

            if (user.ExternalUserId == User.Identity.Name &&
                model.Role != UserRoles.Admin)
            {
                var adminCount = await db.Users.CountAsync(u => u.Role == UserRoles.Admin);
                if (adminCount <= 1)
                {
                    ModelState.AddModelError("", "Cannot demote the last administrator. Please promote another user to admin first.");
                    return View(model);
                }
            }

            user.FullName = model.FullName;
            user.Role = model.Role;
            user.ExternalRoleId = model.ExternalRoleId;

            await db.SaveChangesAsync();

            TempData["SuccessMessage"] = $"User '{user.FullName}' updated successfully.";
            return RedirectToAction("Users");
        }

        // GET: /AdminDashboard/SystemSettings
        public async Task<ActionResult> SystemSettings()
        {
            var uploadPath = UploadConfiguration.FinalUploadPath;
            DriveInfo uploadDrive = null;
            try
            {
                var driveLetter = Path.GetPathRoot(uploadPath);
                uploadDrive = new DriveInfo(driveLetter);
            }
            catch { }

            var totalFilesSize = await db.Files
                .Where(f => !f.IsProcessing)
                .Select(f => (long?)f.Size)
                .SumAsync();

            var totalStorageBytes = totalFilesSize ?? 0;

            var tusStorePath = HostingEnvironment.MapPath("~/App_Data/Tus/Store");

            var totalUsers = await db.Users.CountAsync();
            var totalAdminUsers = await db.Users.CountAsync(u => u.Role == UserRoles.Admin);
            var totalFiles = await db.Files.CountAsync(f => !f.IsProcessing);
            var activeUploadSessions = await db.Files.CountAsync(f => f.IsProcessing);
            var totalFolders = await db.Folders.CountAsync();

            var allowedExts = await db.AllowedFileExtensions
                .Where(e => e.IsEnabled)
                .OrderBy(e => e.Extension)
                .Select(e => e.Extension)
                .ToListAsync();

            var settings = new SystemSettingsViewModel
            {
                ChunkSizeInBytes = UploadConfiguration.ChunkSizeInBytes,
                ChunkSizeInMB = UploadConfiguration.ChunkSizeInBytes / (1024.0 * 1024.0),
                MaxChunkRetries = UploadConfiguration.MaxChunkRetries,
                ChecksumSamplingThresholdBytes = UploadConfiguration.ChecksumSamplingThresholdBytes,
                ChecksumSamplingThresholdMB = UploadConfiguration.ChecksumSamplingThresholdBytes / (1024.0 * 1024.0),
                ChecksumSampleSizeBytes = UploadConfiguration.ChecksumSampleSizeBytes,
                ChecksumSampleSizeKB = UploadConfiguration.ChecksumSampleSizeBytes / 1024.0,
                FileStreamBufferSize = UploadConfiguration.FileStreamBufferSize,
                DefaultUserRoleId = UploadConfiguration.DefaultUserRoleId,
                ActiveDirectoryDomain = UploadConfiguration.ActiveDirectoryDomain,
                FinalUploadPath = UploadConfiguration.FinalUploadPath,
                TempChunkPath = UploadConfiguration.TempChunkPath,
                TempMergePath = UploadConfiguration.TempMergePath,
                TusStorePath = tusStorePath,
                DatabaseConnectionString = MaskConnectionString(ConfigurationManager.ConnectionStrings["CloudStorageDbContext"]?.ConnectionString),
                HangfireConnectionString = MaskConnectionString(ConfigurationManager.ConnectionStrings["HangfireDB"]?.ConnectionString),
                ServerName = Environment.MachineName,
                ApplicationVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString(),
                FrameworkVersion = Environment.Version.ToString(),
                ServerTimeUtc = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"),
                ServerTimeLocal = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                ApplicationPool = Environment.GetEnvironmentVariable("APP_POOL_ID") ?? "N/A",
                UploadDriveTotalSpaceGB = uploadDrive != null ? uploadDrive.TotalSize / (1024.0 * 1024.0 * 1024.0) : 0,
                UploadDriveFreeSpaceGB = uploadDrive != null ? uploadDrive.AvailableFreeSpace / (1024.0 * 1024.0 * 1024.0) : 0,
                UploadDriveUsedSpaceGB = uploadDrive != null ? (uploadDrive.TotalSize - uploadDrive.AvailableFreeSpace) / (1024.0 * 1024.0 * 1024.0) : 0,
                UploadDriveUsagePercent = uploadDrive != null ? ((double)(uploadDrive.TotalSize - uploadDrive.AvailableFreeSpace) / uploadDrive.TotalSize) * 100 : 0,
                TotalUsers = totalUsers,
                TotalAdminUsers = totalAdminUsers,
                TotalFiles = totalFiles,
                TotalStorageUsedGB = totalStorageBytes / (1024.0 * 1024.0 * 1024.0),
                ActiveUploadSessions = activeUploadSessions,
                TotalFolders = totalFolders,
                HangfireWorkersDefault = Math.Max(2, Environment.ProcessorCount),
                HangfireWorkersZip = Math.Max(2, Environment.ProcessorCount * 2),
                HangfireWorkersMaintenance = Math.Max(1, Environment.ProcessorCount / 2),
                AllowedExtensionsList = allowedExts,
                AllowedExtensionsRaw = string.Join(", ", allowedExts)
            };

            return View(settings);
        }

        // POST: /AdminDashboard/SystemSettings
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> SystemSettings(SystemSettingsViewModel model)
        {
            if (!ModelState.IsValid)
            {
                model.AllowedExtensionsList = await db.AllowedFileExtensions
                    .Where(e => e.IsEnabled)
                    .OrderBy(e => e.Extension)
                    .Select(e => e.Extension)
                    .ToListAsync();

                return View(model);
            }

            var raw = model.AllowedExtensionsRaw ?? string.Empty;
            var tokens = raw
                .Split(new[] { ',', ';', ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(UploadConfiguration.NormalizeExt)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var existing = await db.AllowedFileExtensions.ToListAsync();
            db.AllowedFileExtensions.RemoveRange(existing);

            int? currentUserId = null;
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (user != null) currentUserId = user.Id;

            foreach (var ext in tokens)
            {
                db.AllowedFileExtensions.Add(new AllowedFileExtension
                {
                    Extension = ext,
                    IsEnabled = true,
                    CreatedAt = DateTime.UtcNow,
                    CreatedByUserId = currentUserId
                });
            }

            await db.SaveChangesAsync();

            TempData["SuccessMessage"] = "Allowed upload extensions updated.";
            return RedirectToAction("SystemSettings");
        }

        // GET: /AdminDashboard/ClientApplications
        public async Task<ActionResult> ClientApplications()
        {
            var appsQuery = from ca in db.ClientApplications
                            join u in db.Users on ca.CreatedByUserId equals u.Id into userJoin
                            from u in userJoin.DefaultIfEmpty()
                            orderby ca.CreatedAt descending
                            select new ClientApplicationViewModel
                            {
                                Id = ca.Id,
                                ApiId = ca.ApiId,
                                ApplicationName = ca.ApplicationName,
                                ApplicationUrl = ca.ApplicationUrl,
                                ServerAddress = ca.ServerAddress,
                                CreatedAt = ca.CreatedAt,
                                CreatedByUserFullName = u != null ? u.FullName : "Unknown"
                            };

            var viewModel = await appsQuery.ToListAsync();

            return View(viewModel);
        }

        // POST: /AdminDashboard/DeleteClientApp
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteClientApp(int id)
        {
            var app = await db.ClientApplications.FindAsync(id);
            if (app == null)
            {
                return Json(new { success = false, message = "Application not found." });
            }

            db.ClientApplications.Remove(app);
            await db.SaveChangesAsync();

            return Json(new { success = true, message = "Client application deleted successfully." });
        }

        private string MaskConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return "Not configured";

            try
            {
                var masked = System.Text.RegularExpressions.Regex.Replace(
                    connectionString,
                    @"(password|pwd)=([^;]+)",
                    "$1=****",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );

                return masked;
            }
            catch
            {
                return "Error reading connection string";
            }
        }

        // ----------------- MULTI-DEPARTMENT CREATE (VISUAL HIERARCHY) -----------------


        /// <summary>
        /// Get an existing DepartmentNameCatalog.Id for the provided name (case-insensitive),
        /// or insert a new catalog row and return its Id. Creates fresh SqlParameter objects
        /// for each DB call to avoid "SqlParameter is already contained by another SqlParameterCollection".
        /// </summary>
        private async Task<int> GetOrCreateDepartmentNameIdAsync(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("name");

            name = name.Trim();

            // 1) Try to get existing id (use fresh parameter)
            var selectParam = new SqlParameter("@name", name);
            var existingId = await db.Database.SqlQuery<int?>(
                "SELECT TOP 1 Id FROM dbo.DepartmentNameCatalog WHERE LOWER([Name]) = LOWER(@name)",
                selectParam
            ).FirstOrDefaultAsync();

            if (existingId.HasValue)
                return existingId.Value;

            // 2) Insert new name (use a new parameter instance)
            var insertParam = new SqlParameter("@name", name);
            await db.Database.ExecuteSqlCommandAsync(
                "INSERT INTO dbo.DepartmentNameCatalog ([Name], CreatedAt) VALUES (@name, GETUTCDATE())",
                insertParam
            );

            // 3) Read back the id for the inserted row (new parameter again)
            var select2Param = new SqlParameter("@name", name);
            var newId = await db.Database.SqlQuery<int>(
                "SELECT TOP 1 Id FROM dbo.DepartmentNameCatalog WHERE LOWER([Name]) = LOWER(@name) ORDER BY Id DESC",
                select2Param
            ).FirstOrDefaultAsync();

            return newId;
        }



        // GET: AdminDashboard/CreateDepartment
        [HttpGet]
        [Authorize]
        public async Task<ActionResult> CreateDepartment()
        {
            var vm = new DepartmentBulkHierarchyViewModel();

            var exts = await db.AllowedFileExtensions
                .Where(e => e.IsEnabled)
                .OrderBy(e => e.Extension)
                .ToListAsync();

            ViewBag.AvailableExtensions = exts;

            var defaults = new DepartmentDefaultsViewModel
            {
                IsVisibilityAllowed = true,
                IsDownloadingAllowed = true,
                IsZippingAllowed = true
            };
            ViewBag.Defaults = defaults;

            return View(vm);
        }

        // POST: AdminDashboard/CreateDepartment
        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        public async Task<ActionResult> CreateDepartment(string HierarchyJson, DepartmentDefaultsViewModel defaults)
        {
            var vm = new DepartmentBulkHierarchyViewModel();

            var exts = await db.AllowedFileExtensions
                .Where(e => e.IsEnabled)
                .OrderBy(e => e.Extension)
                .ToListAsync();
            ViewBag.AvailableExtensions = exts;
            ViewBag.Defaults = defaults ?? new DepartmentDefaultsViewModel();

            if (string.IsNullOrWhiteSpace(HierarchyJson))
            {
                ModelState.AddModelError("", "Please define at least one department in the hierarchy using the tree.");
                return View(vm);
            }

            List<HierarchyNodeDto> nodes;
            try
            {
                nodes = Newtonsoft.Json.JsonConvert.DeserializeObject<List<HierarchyNodeDto>>(HierarchyJson);
            }
            catch
            {
                ModelState.AddModelError("", "Could not read the hierarchy data. Please try again.");
                return View(vm);
            }

            if (nodes == null || nodes.Count == 0)
            {
                ModelState.AddModelError("", "Please define at least one department in the hierarchy.");
                return View(vm);
            }

            // Collect distinct paths
            var paths = new List<string>();
            void CollectPaths(IEnumerable<HierarchyNodeDto> list)
            {
                if (list == null) return;
                foreach (var n in list)
                {
                    if (!string.IsNullOrWhiteSpace(n.path))
                        paths.Add(n.path);

                    if (n.children != null && n.children.Count > 0)
                        CollectPaths(n.children);
                }
            }
            CollectPaths(nodes);
            paths = paths.Distinct().ToList();

            bool hasDefaultExtensions = defaults?.ExtensionIds != null && defaults.ExtensionIds.Length > 0;

            // IMPORTANT:
            // We expect the DB schema to have Departments.DepartmentNameCatalogId column and DepartmentNameCatalog table.
            // Run the provided SQL script and update EF model before using code below that sets Department.DepartmentNameCatalogId.
            foreach (var path in paths)
            {
                var segments = path
                    .Split(new[] { '>' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(p => p.Trim())
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .ToList();

                if (!segments.Any())
                    continue;

                Department parent = null;

                foreach (var segment in segments)
                {
                    // Ensure Name exists in catalog and get its id
                    var nameId = await GetOrCreateDepartmentNameIdAsync(segment);

                    int? parentId = parent == null ? (int?)null : parent.Id;

                    // Try to find existing department by catalog id + parent
                    var existing = await db.Departments
                        .FirstOrDefaultAsync(d =>
                            d.DepartmentNameCatalogId == nameId &&
                            d.ParentDepartmentId == parentId);

                    if (existing == null)
                    {
                        // Create new department and link to name catalog
                        var newDept = new Department
                        {
                            // keep human readable Name for compatibility
                            Name = segment,
                            DepartmentNameCatalogId = nameId,
                            ParentDepartmentId = parentId,
                            IsVisibilityAllowed = defaults?.IsVisibilityAllowed ?? false,
                            IsDownloadingAllowed = defaults?.IsDownloadingAllowed ?? false,
                            IsZippingAllowed = defaults?.IsZippingAllowed ?? false,
                            IsSharingAllowed = defaults?.IsSharingAllowed ?? false,
                            IsExternalSharingAllowed = defaults?.IsExternalSharingAllowed ?? false,
                            IsCopyFromOtherDriveAllowed = defaults?.IsCopyFromOtherDriveAllowed ?? false,
                            CreatedAt = DateTime.UtcNow
                        };

                        db.Departments.Add(newDept);
                        await db.SaveChangesAsync();

                        // Apply default extensions if present
                        if (hasDefaultExtensions)
                        {
                            foreach (var extId in defaults.ExtensionIds)
                            {
                                var ext = await db.AllowedFileExtensions.FindAsync(extId);
                                if (ext != null)
                                {
                                    newDept.AllowedFileExtensions.Add(ext);
                                }
                            }
                            await db.SaveChangesAsync();
                        }

                        existing = newDept;
                    }

                    parent = existing;
                }
            }

            TempData["SuccessMessage"] = "Department hierarchy created successfully with selected policies and extensions.";
            return RedirectToAction("Departments");
        }

        // ----------------- OPTIONAL: OLD BULK MANUAL PATH TOOL -----------------

        [HttpGet]
        [Authorize]
        public ActionResult ManageDepartmentHierarchy()
        {
            var vm = new DepartmentBulkHierarchyViewModel();
            vm.Rows.Add(new DepartmentPathRowViewModel());
            vm.Rows.Add(new DepartmentPathRowViewModel());
            vm.Rows.Add(new DepartmentPathRowViewModel());
            return View(vm);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        public async Task<ActionResult> ManageDepartmentHierarchy(DepartmentBulkHierarchyViewModel model)
        {
            if (model == null || model.Rows == null)
            {
                ModelState.AddModelError("", "No data submitted.");
                return View(new DepartmentBulkHierarchyViewModel());
            }

            var rows = model.Rows
                .Where(r => !string.IsNullOrWhiteSpace(r.Path))
                .ToList();

            if (!rows.Any())
            {
                ModelState.AddModelError("", "Please enter at least one department path.");
                return View(model);
            }

            if (db == null)
            {
                ModelState.AddModelError("", "Database context is not initialized in AdminDashboardController.");
                return View(model);
            }

            foreach (var row in rows)
            {
                if (row == null || string.IsNullOrWhiteSpace(row.Path))
                    continue;

                var segments = row.Path
                    .Split(new[] { '>' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(p => p == null ? null : p.Trim())
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .ToList();

                if (!segments.Any())
                    continue;

                Department parent = null;

                foreach (var segment in segments)
                {
                    if (segment == null) continue;

                    int? parentId = parent == null ? (int?)null : parent.Id;

                    var existing = await db.Departments
                        .FirstOrDefaultAsync(d =>
                            d.Name == segment &&
                            d.ParentDepartmentId == parentId);

                    if (existing == null)
                    {
                        existing = new Department
                        {
                            Name = segment,
                            ParentDepartmentId = parentId,
                            IsVisibilityAllowed = true,
                            IsDownloadingAllowed = true,
                            IsZippingAllowed = true,
                            IsSharingAllowed = false,
                            IsExternalSharingAllowed = false,
                            IsCopyFromOtherDriveAllowed = false,
                            CreatedAt = DateTime.UtcNow
                        };
                        db.Departments.Add(existing);
                        await db.SaveChangesAsync();
                    }

                    parent = existing;
                }

                if (parent != null && !string.IsNullOrWhiteSpace(row.DataMonitorExternalUserId))
                {
                    var externalId = row.DataMonitorExternalUserId.Trim();

                    try
                    {
                        if (!AdHelper.UserExists(externalId))
                        {
                            ModelState.AddModelError("",
                                $"AD user '{externalId}' does not exist for path '{row.Path}'. No changes were made for this row.");
                            return View(model);
                        }
                    }
                    catch (Exception ex)
                    {
                        ModelState.AddModelError("",
                            $"Error validating AD user '{externalId}' for path '{row.Path}': {ex.Message}");
                        return View(model);
                    }

                    var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
                    if (user == null)
                    {
                        user = new User
                        {
                            ExternalUserId = externalId,
                            Username = externalId,
                            FullName = externalId,
                            Role = UserRoles.User,
                            CreatedAt = DateTime.UtcNow,
                            ExternalRoleId = UploadConfiguration.DefaultUserRoleId,
                            PasswordHash = System.Web.Helpers.Crypto.HashPassword(Guid.NewGuid().ToString("N")),
                            IsDepartmentApproved = false,
                            IsDepartmentRejected = false
                        };
                        db.Users.Add(user);
                        await db.SaveChangesAsync();
                    }

                    parent.DataMonitorUserId = user.Id;
                    await db.SaveChangesAsync();
                }
            }

            TempData["SuccessMessage"] = "Department hierarchy saved successfully.";
            return RedirectToAction("Departments");
        }

        // ----------------- DEPARTMENTS LIST & EDIT -----------------

        // GET: /AdminDashboard/Departments
        // Show only root departments (ParentDepartmentId == null)
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> Departments()
        {
            var roots = await db.Departments
                .Where(d => d.ParentDepartmentId == null)
                .OrderBy(d => d.Name)
                .ToListAsync();

            return View("_DepartmentIndex", roots);
        }


        // GET: /AdminDashboard/ShowDepartmentTree/5
        // Display the full tree for the chosen root department (all descendants)
        [HttpGet]
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> ShowDepartmentTree(int id)
        {
            // Load all departments so we can build the subtree in memory
            var all = await db.Departments
                .Include(d => d.User)
                .Include(d => d.AllowedFileExtensions)
                .ToListAsync();

            var root = all.FirstOrDefault(d => d.Id == id);
            if (root == null) return HttpNotFound();

            // Build lookup parentId -> children
            var lookup = all.GroupBy(d => d.ParentDepartmentId ?? 0)
                            .ToDictionary(g => g.Key, g => g.ToList());

            ViewBag.Lookup = lookup;
            ViewBag.RootId = root.Id;
            ViewBag.RootName = root.Name;

            return View("DepartmentTree", root);
        }


        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> CreateChild(int parentId, string childName)
        {
            // Validate name
            if (string.IsNullOrWhiteSpace(childName))
            {
                TempData["ErrorMessage"] = "Child department name is required.";
                var rootIfErr = await GetRootIdForParent(parentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootIfErr });
            }

            childName = childName.Trim();

            // Load parent (including allowed extensions)
            var parent = await db.Departments
                                 .Include(d => d.AllowedFileExtensions)
                                 .FirstOrDefaultAsync(d => d.Id == parentId);

            if (parent == null)
            {
                TempData["ErrorMessage"] = "Parent department not found.";
                // fallback to top-level view
                return RedirectToAction("Departments");
            }

            // Prevent duplicate child under same parent
            var duplicate = await db.Departments.AnyAsync(d => d.ParentDepartmentId == parentId && d.Name == childName);
            if (duplicate)
            {
                TempData["ErrorMessage"] = $"A department named '{childName}' already exists under '{parent.Name}'.";
                var rootDup = await GetRootIdForParent(parentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootDup });
            }

            // Create child inheriting policies from parent
            var child = new Department
            {
                Name = childName,
                ParentDepartmentId = parentId,
                CreatedAt = DateTime.UtcNow,

                // inherit policy flags
                IsVisibilityAllowed = parent.IsVisibilityAllowed,
                IsDownloadingAllowed = parent.IsDownloadingAllowed,
                IsZippingAllowed = parent.IsZippingAllowed,
                IsSharingAllowed = parent.IsSharingAllowed,
                IsExternalSharingAllowed = parent.IsExternalSharingAllowed,
                IsCopyFromOtherDriveAllowed = parent.IsCopyFromOtherDriveAllowed,

                AllowedFileExtensions = new List<AllowedFileExtension>()
            };

            // Attach parent's allowed extensions (by reference to existing entities)
            if (parent.AllowedFileExtensions != null && parent.AllowedFileExtensions.Any())
            {
                foreach (var ext in parent.AllowedFileExtensions)
                {
                    // ensure we attach the tracked entity from the context
                    var existing = await db.AllowedFileExtensions.FindAsync(ext.Id);
                    if (existing != null)
                    {
                        child.AllowedFileExtensions.Add(existing);
                    }
                }
            }

            db.Departments.Add(child);
            await db.SaveChangesAsync();

            TempData["SuccessMessage"] = $"Department '{childName}' created under '{parent.Name}' (inherited policies & allowed extensions).";

            var rootId = await GetRootIdForParent(parentId);
            return RedirectToAction("ShowDepartmentTree", new { id = rootId });
        }



        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> AssignDataMonitor(int departmentId, string externalUserId)
        {
            if (string.IsNullOrWhiteSpace(externalUserId))
            {
                TempData["ErrorMessage"] = "Data monitor AD user id is required.";
                var rootIdX = await GetRootIdForParent(departmentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootIdX });
            }

            var dept = await db.Departments.FindAsync(departmentId);
            if (dept == null) return HttpNotFound();

            var externalId = externalUserId.Trim();

            // Do NOT auto-create user. Require user to have logged in at least once.
            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
            if (user == null)
            {
                TempData["ErrorMessage"] = $"User '{externalId}' is not registered in the system. Please ask the user to sign in at least once so an account can be created before assigning them as Data Monitor.";
                var rootId = await GetRootIdForParent(departmentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootId });
            }

            // User must be department-approved before becoming Data Monitor
            if (!user.IsDepartmentApproved)
            {
                TempData["ErrorMessage"] =
                    $"User '{externalId}' is not department-approved. " +
                    "Only approved users can be assigned as Data Monitor.";
                var rootIdNotApproved = await GetRootIdForParent(departmentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootIdNotApproved });
            }

            // NEW RULE: user must belong to THIS department
            if (!user.DepartmentId.HasValue || user.DepartmentId.Value != dept.Id)
            {
                TempData["ErrorMessage"] =
                    $"User '{externalId}' does not belong to department '{dept.Name}'. " +
                    "A Data Monitor can only be assigned to their own department.";
                var rootIdWrongDept = await GetRootIdForParent(departmentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootIdWrongDept });
            }

            // NEW: ensure this user is not already monitor of another department
            var otherDept = await db.Departments
                .FirstOrDefaultAsync(d => d.DataMonitorUserId == user.Id && d.Id != dept.Id);

            if (otherDept != null)
            {
                TempData["ErrorMessage"] =
                    $"User '{externalId}' is already assigned as Data Monitor to department '{otherDept.Name}'. " +
                    "A user can only be Data Monitor for one department.";
                var rootIdDup = await GetRootIdForParent(departmentId);
                return RedirectToAction("ShowDepartmentTree", new { id = rootIdDup });
            }

            dept.DataMonitorUserId = user.Id;
            await db.SaveChangesAsync();

            TempData["SuccessMessage"] = $"Data monitor '{user.FullName}' assigned to '{dept.Name}'.";

            var rootId2 = await GetRootIdForParent(departmentId);
            return RedirectToAction("ShowDepartmentTree", new { id = rootId2 });
        }



        // Helper: find the top-level root id for a given department id (walk up parents)
        private async Task<int> GetRootIdForParent(int departmentId)
        {
            var current = await db.Departments.FindAsync(departmentId);
            if (current == null) return departmentId;
            var safety = 0;
            while (current.ParentDepartmentId.HasValue && safety++ < 100)
            {
                current = await db.Departments.FindAsync(current.ParentDepartmentId.Value);
                if (current == null) break;
            }
            return current != null ? current.Id : departmentId;
        }

        [Authorize]
        public async Task<ActionResult> EditDepartment(int id)
        {
            var dept = await db.Departments
                .Include(d => d.AllowedFileExtensions)
                .Include(d => d.User)
                .FirstOrDefaultAsync(d => d.Id == id);

            if (dept == null) return HttpNotFound();

            var allDepts = await db.Departments
                .OrderBy(d => d.Name)
                .ToListAsync();

            var vm = new DepartmentCreateViewModel
            {
                Id = dept.Id,
                Name = dept.Name,
                ParentDepartmentId = dept.ParentDepartmentId,
                DataMonitorExternalUserId = dept.User != null ? dept.User.ExternalUserId : null,
                IsVisibilityAllowed = dept.IsVisibilityAllowed,
                IsZippingAllowed = dept.IsZippingAllowed,
                IsDownloadingAllowed = dept.IsDownloadingAllowed,
                IsSharingAllowed = dept.IsSharingAllowed,
                IsExternalSharingAllowed = dept.IsExternalSharingAllowed,
                IsCopyFromOtherDriveAllowed = dept.IsCopyFromOtherDriveAllowed,
                AvailableExtensions = new MultiSelectList(
                    await db.AllowedFileExtensions.Where(e => e.IsEnabled).ToListAsync(),
                    "Id",
                    "Extension",
                    dept.AllowedFileExtensions.Select(e => e.Id)),
                ParentDepartments = new SelectList(
                    allDepts.Where(d => d.Id != id), "Id", "Name", dept.ParentDepartmentId)
            };

            // Build tree for the view
            var dict = allDepts.ToDictionary(d => d.Id);
            Func<Department, string> buildPath = null;
            buildPath = d =>
            {
                var names = new List<string>();
                var current = d;
                var safety = 0;
                while (current != null && safety++ < 50)
                {
                    names.Add(current.Name);
                    if (!current.ParentDepartmentId.HasValue) break;
                    Department parent;
                    if (!dict.TryGetValue(current.ParentDepartmentId.Value, out parent)) break;
                    current = parent;
                }
                names.Reverse();
                return string.Join(" > ", names);
            };

            var treeItems = allDepts
                .Select(d => new
                {
                    d.Id,
                    d.ParentDepartmentId,
                    Name = d.Name,
                    FullPath = buildPath(d)
                })
                .OrderBy(x => x.FullPath)
                .ToList();

            ViewBag.DepartmentTree = treeItems;
            ViewBag.CurrentDepartmentId = id;

            return View(vm); // Views/AdminDashboard/EditDepartment.cshtml
        }





        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> EditDepartment(DepartmentCreateViewModel model)
        {
            if (!ModelState.IsValid)
            {
                // re-populate select lists as needed before returning the view
                var depts = await db.Departments.OrderBy(d => d.Name).ToListAsync();
                model.ParentDepartments = new SelectList(depts, "Id", "Name", model.ParentDepartmentId);
                model.AvailableExtensions = new SelectList(
                    await db.AllowedFileExtensions.OrderBy(e => e.Extension).ToListAsync(),
                    "Id",
                    "Extension",
                    model.SelectedExtensionIds);
                return View(model);
            }

            var department = await db.Departments.FindAsync(model.Id);
            if (department == null) return HttpNotFound();

            // Basic fields
            department.Name = model.Name;
            department.ParentDepartmentId = model.ParentDepartmentId;
            department.IsVisibilityAllowed = model.IsVisibilityAllowed;
            department.IsDownloadingAllowed = model.IsDownloadingAllowed;
            department.IsZippingAllowed = model.IsZippingAllowed;
            department.IsSharingAllowed = model.IsSharingAllowed;
            department.IsExternalSharingAllowed = model.IsExternalSharingAllowed;
            department.IsCopyFromOtherDriveAllowed = model.IsCopyFromOtherDriveAllowed;

            // Data monitor assignment with rules:
            // 1) User must exist.
            // 2) User.DepartmentId must be this department.Id.
            // 3) User must not already be monitor of another department.
            if (!string.IsNullOrWhiteSpace(model.DataMonitorExternalUserId))
            {
                var externalId = model.DataMonitorExternalUserId.Trim();

                var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalId);
                if (user == null)
                {

                    
                    if (!user.IsDepartmentApproved)
                    {
                        ModelState.AddModelError("DataMonitorExternalUserId",
                            $"User '{externalId}' is not department-approved. Only approved users can be Data Monitor.");
                        // re-populate selects and return View(model) as you already do in other error branches
                    }

                    ModelState.AddModelError("DataMonitorExternalUserId",
                        "The AD user is not registered in BOBDrive. The user must sign in at least once to create their account before assignment.");

                    var depts2 = await db.Departments.OrderBy(d => d.Name).ToListAsync();
                    model.ParentDepartments = new SelectList(depts2, "Id", "Name", model.ParentDepartmentId);
                    model.AvailableExtensions = new SelectList(
                        await db.AllowedFileExtensions.OrderBy(e => e.Extension).ToListAsync(),
                        "Id",
                        "Extension",
                        model.SelectedExtensionIds);
                    return View(model);
                }

                // Must belong to this department
                if (!user.DepartmentId.HasValue || user.DepartmentId.Value != department.Id)
                {
                    ModelState.AddModelError("DataMonitorExternalUserId",
                        $"User '{externalId}' does not belong to department '{department.Name}'. " +
                        "A Data Monitor can only be assigned to their own department.");

                    var depts2 = await db.Departments.OrderBy(d => d.Name).ToListAsync();
                    model.ParentDepartments = new SelectList(depts2, "Id", "Name", model.ParentDepartmentId);
                    model.AvailableExtensions = new SelectList(
                        await db.AllowedFileExtensions.OrderBy(e => e.Extension).ToListAsync(),
                        "Id",
                        "Extension",
                        model.SelectedExtensionIds);
                    return View(model);
                }

                // Must not already be monitor elsewhere
                var otherDept = await db.Departments
                    .FirstOrDefaultAsync(d => d.DataMonitorUserId == user.Id && d.Id != department.Id);

                if (otherDept != null)
                {
                    ModelState.AddModelError("DataMonitorExternalUserId",
                        $"User '{externalId}' is already assigned as Data Monitor to department '{otherDept.Name}'. " +
                        "A user can only be Data Monitor for one department.");

                    var depts2 = await db.Departments.OrderBy(d => d.Name).ToListAsync();
                    model.ParentDepartments = new SelectList(depts2, "Id", "Name", model.ParentDepartmentId);
                    model.AvailableExtensions = new SelectList(
                        await db.AllowedFileExtensions.OrderBy(e => e.Extension).ToListAsync(),
                        "Id",
                        "Extension",
                        model.SelectedExtensionIds);
                    return View(model);
                }

                department.DataMonitorUserId = user.Id;
            }
            else
            {
                // Clear monitor
                department.DataMonitorUserId = null;
            }

            // Save allowed extensions
            department.AllowedFileExtensions.Clear();
            if (model.SelectedExtensionIds != null && model.SelectedExtensionIds.Any())
            {
                var exts = await db.AllowedFileExtensions
                    .Where(e => model.SelectedExtensionIds.Contains(e.Id))
                    .ToListAsync();
                foreach (var e in exts)
                {
                    department.AllowedFileExtensions.Add(e);
                }
            }

            await db.SaveChangesAsync();

            TempData["SuccessMessage"] = "Department saved.";
            return RedirectToAction("Departments");
        }

        // ----------------- APPROVALS -----------------

        // ----------------- APPROVALS -----------------



        /// <summary>
        /// Given a user with DepartmentId, walk up the department tree and find the nearest
        /// ancestor (including its own department) that has a DataMonitorUserId assigned.
        /// Returns that monitor's UserId, or null if none found.
        /// </summary>
        private async Task<int?> GetResponsibleMonitorUserIdForUserAsync(User user)
        {
            if (user == null || !user.DepartmentId.HasValue)
                return null;

            // Load all departments once (small tree in most orgs)
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






        // ----------------- APPROVALS -----------------

        public async Task<ActionResult> PendingApprovals()
        {
            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            // Base query: all users who chose a department and are not yet approved
            var usersQuery = db.Users
                .Include(u => u.Department)
                .Where(u => u.DepartmentId != null && !u.IsDepartmentApproved);

            if (currentUser.Role == UserRoles.Admin)
            {
                // Admin: see all pending requests
                var allPending = await usersQuery.ToListAsync();
                return View(allPending);
            }
            else
            {
                // Data monitor (or regular user)
                // We only want users whose "responsible monitor" is this current user

                var allPending = await usersQuery.ToListAsync();
                var result = new List<User>();

                foreach (var u in allPending)
                {
                    var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(u);
                    if (responsibleMonitorId.HasValue && responsibleMonitorId.Value == currentUser.Id)
                    {
                        result.Add(u);
                    }
                }

                if (!result.Any())
                {
                    // Not responsible for any pending users
                    return new HttpStatusCodeResult(403, "Not a monitor for any pending requests.");
                }

                return View(result);
            }
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> ApproveUser(int userId)
        {
            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var user = await db.Users.Include(u => u.Department).FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null) return HttpNotFound();

            if (currentUser.Role != UserRoles.Admin)
            {
                // Non-admin: must be the responsible monitor for this user
                var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(user);
                if (!responsibleMonitorId.HasValue || responsibleMonitorId.Value != currentUser.Id)
                {
                    return new HttpStatusCodeResult(403, "You are not the responsible data monitor for this user.");
                }
            }

            await db.Database.ExecuteSqlCommandAsync(
                "UPDATE dbo.Users SET IsDepartmentApproved = 1, IsDepartmentRejected = 0 WHERE Id = @p0",
                userId);

            AdminStatsHub.BroadcastApprovalsChanged();

            TempData["SuccessMessage"] = "User approved.";

            if (Request.IsAjaxRequest())
            {
                return Json(new { success = true, message = "User approved." });
            }

            return RedirectToAction("PendingApprovals");
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> RejectUser(int userId)
        {
            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null)
            {
                return RedirectToAction("Login", "Account");
            }

            var user = await db.Users.Include(u => u.Department).FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null) return HttpNotFound();

            if (currentUser.Role != UserRoles.Admin)
            {
                // Non-admin: must be the responsible monitor for this user
                var responsibleMonitorId = await GetResponsibleMonitorUserIdForUserAsync(user);
                if (!responsibleMonitorId.HasValue || responsibleMonitorId.Value != currentUser.Id)
                {
                    return new HttpStatusCodeResult(403, "You are not the responsible data monitor for this user.");
                }
            }

            await db.Database.ExecuteSqlCommandAsync(
                "UPDATE dbo.Users SET IsDepartmentRejected = 1, IsDepartmentApproved = 0, DepartmentId = NULL WHERE Id = @p0",
                userId);

            AdminStatsHub.BroadcastApprovalsChanged();

            TempData["SuccessMessage"] = "User rejected. They will be asked to select a department again.";

            if (Request.IsAjaxRequest())
            {
                return Json(new { success = true, message = "User rejected." });
            }

            return RedirectToAction("PendingApprovals");
        }




        [HttpGet]
        public async Task<ActionResult> PendingApprovalsPartial()
        {
            var currentUser = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == User.Identity.Name);
            if (currentUser == null)
            {
                return new HttpStatusCodeResult(401);
            }

            IQueryable<User> query = db.Users
                .Include(u => u.Department)
                .Where(u => u.DepartmentId != null && !u.IsDepartmentApproved)
                .OrderBy(u => u.CreatedAt);

            if (currentUser.Role != UserRoles.Admin)
            {
                // Limit partial view too, for data monitors
                var monitoredDeptId = await db.Departments
                    .Where(d => d.DataMonitorUserId == currentUser.Id)
                    .Select(d => (int?)d.Id)
                    .FirstOrDefaultAsync();

                if (!monitoredDeptId.HasValue)
                {
                    return new HttpStatusCodeResult(403, "Not a monitor");
                }

                var deptId = monitoredDeptId.Value;
                query = query.Where(u => u.DepartmentId == deptId);
            }

            var pending = await query.ToListAsync();

            return PartialView("_PendingApprovalsRows", pending);
        }



        // GET: /AdminDashboard/DepartmentNameSuggestions?q=term
        [HttpGet]
        public async Task<ActionResult> DepartmentNameSuggestions(string q)
        {
            q = (q ?? "").Trim();
            if (q.Length == 0)
                return Json(new string[0], JsonRequestBehavior.AllowGet);

            // Use parameterized query to avoid SQL injection
            var param = new SqlParameter("@p0", "%" + q + "%");
            var results = await db.Database.SqlQuery<string>(
                "SELECT TOP 20 [Name] FROM dbo.DepartmentNameCatalog WHERE [Name] LIKE @p0 ORDER BY [Name]",
                param
            ).ToListAsync();

            return Json(results, JsonRequestBehavior.AllowGet);
        }

        /// <summary>
        /// AJAX: Add a department name to the catalog if not exists. Returns JSON { success, id, name }.
        /// Uses fresh SqlParameter instances for each DB call.
        /// </summary>
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> AddDepartmentName(string name)
        {
            name = (name ?? "").Trim();
            if (string.IsNullOrWhiteSpace(name))
                return Json(new { success = false, message = "Name is required." });

            try
            {
                // Check existing (case-insensitive)
                var existsParam = new SqlParameter("@name", name);
                var existingId = await db.Database.SqlQuery<int?>(
                    "SELECT TOP 1 Id FROM dbo.DepartmentNameCatalog WHERE LOWER([Name]) = LOWER(@name)",
                    existsParam
                ).FirstOrDefaultAsync();

                if (existingId.HasValue)
                    return Json(new { success = true, id = existingId.Value, name = name });

                // Insert new (new parameter)
                var insertParam = new SqlParameter("@name", name);
                await db.Database.ExecuteSqlCommandAsync(
                    "INSERT INTO dbo.DepartmentNameCatalog ([Name], CreatedAt) VALUES (@name, GETUTCDATE())",
                    insertParam
                );

                // Return the inserted id (new parameter)
                var selParam = new SqlParameter("@name", name);
                var id = await db.Database.SqlQuery<int>(
                    "SELECT TOP 1 Id FROM dbo.DepartmentNameCatalog WHERE LOWER([Name]) = LOWER(@name) ORDER BY Id DESC",
                    selParam
                ).FirstOrDefaultAsync();

                return Json(new { success = true, id = id, name = name });
            }
            catch (Exception ex)
            {
                // Avoid exposing exception details in production; log if you have logging
                return Json(new { success = false, message = ex.Message });
            }
        }

        // INSIDE AdminDashboardController class
        [HttpGet]
        [Authorize]
        [AdminAuthorize]
        public async Task<ActionResult> IsUserDataMonitorElsewhere(string externalUserId, int? departmentId)
        {
            externalUserId = (externalUserId ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(externalUserId))
            {
                return Json(new { exists = false, isMonitorElsewhere = false }, JsonRequestBehavior.AllowGet);
            }

            var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
            if (user == null)
            {
                return Json(new { exists = false, isMonitorElsewhere = false }, JsonRequestBehavior.AllowGet);
            }

            // Find any department (other than the current one) where this user is DataMonitorUserId
            var dept = await db.Departments
                .FirstOrDefaultAsync(d => d.DataMonitorUserId == user.Id &&
                                          (!departmentId.HasValue || d.Id != departmentId.Value));

            if (dept == null)
            {
                return Json(new
                {
                    exists = true,
                    isMonitorElsewhere = false
                }, JsonRequestBehavior.AllowGet);
            }

            // Optional: build full path for nicer messages
            string path = dept.Name;
            try
            {
                var allDepts = await db.Departments.ToListAsync();
                var dict = allDepts.ToDictionary(d => d.Id);
                var names = new List<string>();
                var current = dept;
                var safety = 0;
                while (current != null && safety++ < 50)
                {
                    names.Add(current.Name);
                    if (!current.ParentDepartmentId.HasValue) break;
                    Department parent;
                    if (!dict.TryGetValue(current.ParentDepartmentId.Value, out parent)) break;
                    current = parent;
                }
                names.Reverse();
                path = string.Join(" > ", names);
            }
            catch
            {
                // ignore path building errors
            }

            return Json(new
            {
                exists = true,
                isMonitorElsewhere = true,
                departmentId = dept.Id,
                departmentName = dept.Name,
                departmentPath = path
            }, JsonRequestBehavior.AllowGet);
        }


    }
}