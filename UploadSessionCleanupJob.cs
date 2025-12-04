using BOBDrive.App_Start;
using BOBDrive.Models;
using Serilog;
using System;
using System.Threading.Tasks;
using System.Web.Hosting;

namespace BOBDrive.Services.Uploads
{
    public static class UploadSessionCleanupJob
    {
        // Single sweep for ALL sessions (invoked by the recurring job in HangfireConfig)
        public static async Task CleanupTusSessionsAsync()
        {
            try
            {
                var tusStorePath = HostingEnvironment.MapPath("~/App_Data/Tus/Store");
                using (var db = new CloudStorageDbContext())
                {
                    var svc = new UploadSessionService(db, Log.Logger, LoggingConfig.AuditLogger, tusStorePath, "/tus");
                    await svc.PurgeExpiredDeepAsync();
                }
                Log.Information("UploadSessionCleanupJob: sweep completed.");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "UploadSessionCleanupJob: sweep failed.");
            }
        }

        // Backward-compatibility wrappers so legacy scheduled jobs won’t fail if they still exist.
        // These DO NOT schedule anything; they simply delegate to the global sweep.
        public static Task CleanupTusSession() => CleanupTusSessionsAsync();
        public static Task CleanupTusSession(string tusFileId) => CleanupTusSessionsAsync();
    }
}