using Serilog;
using System;
using System.Threading.Tasks;
using BOBDrive.Controllers;
using BOBDrive.Services.FileOps;
using BOBDrive.Services.Uploads;

namespace BOBDrive.Services.Jobs
{
    public static class RecurringJobRunner
    {
        public static async Task ReconcileUploads()
        {
            var log = Log.ForContext("RecurringJob", "reconcile-uploads");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                log.Information("Start");
                await new FileController().ReconcileIncompleteUploads();
                log.Information("Success elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
                throw;
            }
        }

        public static async Task CleanupTusSessions()
        {
            var log = Log.ForContext("RecurringJob", "cleanup-tus-sessions-12h");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                log.Information("Start");
                await UploadSessionCleanupJob.CleanupTusSessionsAsync();
                log.Information("Success elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
                throw;
            }
        }

        public static async Task PurgeBin()
        {
            var log = Log.ForContext("Job", "PurgeBin");
            log.Information("Starting 90-day bin purge job.");

            try
            {
                await DeleteService.PurgeExpiredBinEntries(log);
                log.Information("90-day bin purge job completed successfully.");
            }
            catch (Exception ex)
            {
                log.Error(ex, "90-day bin purge job failed.");
                throw;  // let Hangfire retry
            }
        }

        public static async Task ReconcileFileBlobs()
        {
            var log = Log.ForContext("RecurringJob", "reconcile-fileblobs");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                log.Information("Start");
                await FileBlobsReconciler.ReconcileAllAsync(false);
                log.Information("Success elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
                throw;
            }
        }

        public static async Task CleanupOrphans()
        {
            var log = Log.ForContext("RecurringJob", "cleanup-orphan-physicals");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                log.Information("Start");
                await OrphanCleanupJob.RunAsync();
                log.Information("Success elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed elapsedMs={Elapsed}", sw.ElapsedMilliseconds);
                throw;
            }
        }
    }
}