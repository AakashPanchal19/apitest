using BOBDrive.Models;
using System;
using System.Web.Mvc;
using System.Linq;
using BOBDrive.Controllers;

namespace BOBDrive.Controllers
{
    public class FolderController : BaseController
    {
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(string folderName, int? parentFolderId)
        {
            if (string.IsNullOrWhiteSpace(folderName))
            {
                TempData["ErrorMessage"] = "Folder name cannot be empty.";
                return RedirectToAction("Index", "Home", new { folderId = parentFolderId });
            }

            // Ensure parentFolderId corresponds to an existing folder if not null
            if (parentFolderId.HasValue && !db.Folders.Any(f => f.Id == parentFolderId.Value))
            {
                TempData["ErrorMessage"] = "Invalid parent folder.";
                return RedirectToAction("Index", "Home", new { folderId = (int?)null }); // Redirect to root
            }


            var newFolder = new Folder
            {
                Name = folderName,
                ParentFolderId = parentFolderId,
                CreatedAt = DateTime.Now
            };

            db.Folders.Add(newFolder);
            db.SaveChanges();

            TempData["SuccessMessage"] = "Folder '{folderName}' created successfully.";
            return RedirectToAction("Index", "Home", new { folderId = parentFolderId ?? newFolder.Id });
        }
    }
}