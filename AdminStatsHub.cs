using Microsoft.AspNet.SignalR;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using BOBDrive.Models;

namespace BOBDrive.Hubs
{
    [Authorize]
    public class AdminStatsHub : Hub
    {
        public override Task OnConnected()
        {
            // You can optionally call RequestSnapshot() here for a per-connection initial push.
            return base.OnConnected();
        }

        /// <summary>
        /// Broadcasts current upload/zip stats to ALL connected admin dashboards.
        /// </summary>
        public static async Task BroadcastSnapshot()
        {
            using (var db = new CloudStorageDbContext())
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

                var activeZips = await db.Files
                    .CountAsync(f => f.IsProcessing && f.ContentType == "application/zip");

                var uploadsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType != "application/zip"
                                     && f.UploadedAt >= oneHourAgo);

                var zipsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType == "application/zip"
                                     && f.UploadedAt >= oneHourAgo);

                var context = GlobalHost.ConnectionManager.GetHubContext<AdminStatsHub>();
                context.Clients.All.updateStats(new
                {
                    activeUploads,
                    activeZips,
                    uploadsLastHour,
                    zipsLastHour
                });
            }
        }

        /// <summary>
        /// NEW: Broadcast that pending approvals changed so clients can refresh.
        /// </summary>
        public static void BroadcastApprovalsChanged()
        {
            var context = GlobalHost.ConnectionManager.GetHubContext<AdminStatsHub>();
            context.Clients.All.approvalsChanged();
        }

        /// <summary>
        /// Called from the client to get a fresh snapshot for this caller only.
        /// </summary>
        public async Task RequestSnapshot()
        {
            using (var db = new CloudStorageDbContext())
            {
                var oneHourAgo = DateTime.UtcNow.AddHours(-1);
                var now = DateTime.UtcNow;
                var cutoff = now.AddMinutes(-1);

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

                var activeZips = await db.Files
                    .CountAsync(f => f.IsProcessing && f.ContentType == "application/zip");

                var uploadsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType != "application/zip"
                                     && f.UploadedAt >= oneHourAgo);

                var zipsLastHour = await db.Files
                    .CountAsync(f => !f.IsProcessing
                                     && f.ContentType == "application/zip"
                                     && f.UploadedAt >= oneHourAgo);

                Clients.Caller.updateStats(new
                {
                    activeUploads,
                    activeZips,
                    uploadsLastHour,
                    zipsLastHour
                });
            }
        }
    }
}