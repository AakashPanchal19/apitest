using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Mvc;
using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;

namespace BOBDrive.Controllers
{
    [Authorize]
    public class TusFoldersController : Controller
    {
        public class EnsureRequest
        {
            public List<string> RelativePaths { get; set; }
            public List<string> BaseCandidates { get; set; }
            public bool ForceUniqueBase { get; set; } = false;
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<JsonResult> Ensure(EnsureRequest request)
        {
            try
            {
                var externalUserId = User?.Identity?.Name;
                if (string.IsNullOrWhiteSpace(externalUserId))
                {
                    Response.StatusCode = 401;
                    return Json(new { success = false, message = "Not authenticated." });
                }

                var originalRel = (request?.RelativePaths ?? new List<string>())
                    .Select(p => (p ?? "").Replace('\\', '/').Trim('/'))
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var baseCandidates = (request?.BaseCandidates ?? new List<string>())
                    .Select(b => (b ?? "").Trim())
                    .Where(b => !string.IsNullOrWhiteSpace(b))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                // Empty folder case: ensure base candidate triggers creation
                if (!originalRel.Any() && baseCandidates.Any())
                    originalRel.AddRange(baseCandidates);

                using (var db = new CloudStorageDbContext())
                {
                    var user = await db.Users.FirstOrDefaultAsync(u => u.ExternalUserId == externalUserId);
                    if (user == null)
                        return Json(new { success = false, message = "User not found." });

                    var root = await EnsureUserRootAsync(db, user, CancellationToken.None);
                    if (root == null)
                        return Json(new { success = false, message = "Could not ensure user root." });

                    var userPhysicalRoot = Path.Combine(UploadConfiguration.FinalUploadPath, user.ExternalUserId);
                    Directory.CreateDirectory(userPhysicalRoot);

                    var baseNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var originalBase in baseCandidates)
                    {
                        string finalBase = request.ForceUniqueBase
                            ? await AllocateUniqueBaseAsync(db, originalBase, root.Id)
                            : originalBase;

                        baseNameMap[originalBase] = finalBase;

                        var phys = Path.Combine(userPhysicalRoot, finalBase);
                        if (!Directory.Exists(phys))
                            Directory.CreateDirectory(phys);

                        var existingBase = await db.Folders.FirstOrDefaultAsync(f =>
                            f.ParentFolderId == root.Id && f.Name == finalBase);

                        if (existingBase == null)
                        {
                            existingBase = new Folder
                            {
                                Name = finalBase,
                                ParentFolderId = root.Id,
                                OwnerUserId = user.Id,
                                CreatedAt = DateTime.UtcNow
                            };
                            db.Folders.Add(existingBase);
                            await db.SaveChangesAsync();
                        }
                    }

                    var transformed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var path in originalRel)
                    {
                        var firstSeg = path.Split('/')[0];
                        if (baseNameMap.TryGetValue(firstSeg, out var newBase))
                        {
                            var remainder = path.Length > firstSeg.Length ? path.Substring(firstSeg.Length) : "";
                            transformed.Add((newBase + remainder).Trim('/'));
                        }
                        else
                        {
                            transformed.Add(path);
                        }
                    }

                    foreach (var kv in baseNameMap)
                        transformed.Add(kv.Value);

                    var ordered = transformed
                        .OrderBy(p => p.Count(c => c == '/'))
                        .ThenBy(p => p.Length)
                        .ToList();

                    var pathMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                    foreach (var fullPath in ordered)
                    {
                        var segments = fullPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                        int currentParentId = root.Id;
                        var sb = new StringBuilder();

                        for (int i = 0; i < segments.Length; i++)
                        {
                            var seg = segments[i].Trim();
                            if (seg.Length == 0) continue;

                            if (sb.Length > 0) sb.Append('/');
                            sb.Append(seg);
                            var agg = sb.ToString();

                            var physDir = Path.Combine(userPhysicalRoot, agg.Replace('/', Path.DirectorySeparatorChar));
                            if (!Directory.Exists(physDir))
                                Directory.CreateDirectory(physDir);

                            if (pathMap.TryGetValue(agg, out int existingId))
                            {
                                currentParentId = existingId;
                                continue;
                            }

                            var existing = await db.Folders.FirstOrDefaultAsync(f =>
                                f.ParentFolderId == currentParentId && f.Name == seg);

                            if (existing == null)
                            {
                                existing = new Folder
                                {
                                    Name = seg,
                                    ParentFolderId = currentParentId,
                                    OwnerUserId = user.Id,
                                    CreatedAt = DateTime.UtcNow
                                };
                                db.Folders.Add(existing);
                                await db.SaveChangesAsync();
                            }

                            currentParentId = existing.Id;
                            pathMap[agg] = existing.Id;
                        }
                    }

                    return Json(new
                    {
                        success = true,
                        baseFolderId = root.Id,
                        pathMap,
                        baseNameMap
                    });
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "TusFolders.Ensure failed");
                Response.StatusCode = 500;
                return Json(new { success = false, message = "Server error.", details = ex.Message });
            }
        }

        private static async Task<string> AllocateUniqueBaseAsync(CloudStorageDbContext db, string baseName, int rootId)
        {
            var exists = await db.Folders.AnyAsync(f =>
                f.ParentFolderId == rootId &&
                f.Name.Equals(baseName, StringComparison.OrdinalIgnoreCase));

            if (!exists) return baseName;

            int i = 1;
            while (true)
            {
                var candidate = $"{baseName}({i})";
                var taken = await db.Folders.AnyAsync(f =>
                    f.ParentFolderId == rootId &&
                    f.Name.Equals(candidate, StringComparison.OrdinalIgnoreCase));
                if (!taken) return candidate;
                i++;
            }
        }

        private static async Task<Folder> EnsureUserRootAsync(CloudStorageDbContext db, User user, CancellationToken ct)
        {
            var root = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id, ct);
            if (root != null) return root;

            for (int attempt = 0; attempt < 3; attempt++)
            {
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
                    await db.SaveChangesAsync(ct);
                    return folder;
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException)
                {
                    db.Entry(folder).State = System.Data.Entity.EntityState.Detached;
                    var existing = await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id, ct);
                    if (existing != null) return existing;
                }
                await Task.Delay(100, ct);
            }
            return await db.Folders.FirstOrDefaultAsync(f => f.ParentFolderId == null && f.OwnerUserId == user.Id, ct);
        }
    }
}