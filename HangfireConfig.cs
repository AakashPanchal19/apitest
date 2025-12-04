// File: App_Start/HangfireConfig.cs (COMPLETE UPDATED VERSION)
using BOBDrive.Controllers;
using BOBDrive.Models;
using BOBDrive.Services.FileOps;
using BOBDrive.Services.Jobs;
using BOBDrive.Services.Uploads;
using Hangfire;
using Hangfire.Dashboard;
using Hangfire.SqlServer;
using Hangfire.Storage;
using Microsoft.Owin;
using Owin;
using Serilog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;       // *** ADD THIS ***
using System.IO;
using System.Linq;              // *** ADD THIS ***
using System.Text;
using System.Threading.Tasks;
using System.Web.Hosting;
// tusdotnet
using tusdotnet;
using tusdotnet.Models;
using tusdotnet.Models.Configuration;
using tusdotnet.Models.Expiration;
using tusdotnet.Stores;

[assembly: OwinStartup(typeof(BOBDrive.App_Start.HangfireConfig))]

namespace BOBDrive.App_Start
{
    public class HangfireConfig
    {
        public void Configuration(IAppBuilder app)
        {
            // Keep SignalR mapping
            app.MapSignalR();

            var hangfireCs = ConfigurationManager.ConnectionStrings["HangfireDB"].ConnectionString;

            GlobalConfiguration.Configuration.UseSqlServerStorage(hangfireCs, new SqlServerStorageOptions
            {
                CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
                SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
                QueuePollInterval = TimeSpan.FromSeconds(15),
                UseRecommendedIsolationLevel = true,
                DisableGlobalLocks = true
            });

            app.UseHangfireDashboard("/hangfire", new DashboardOptions
            {
                Authorization = new[] { new HangfireAuthorizationFilter() }  // This uses the updated filter below
            });

            // Existing servers (unchanged)
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                Queues = new[] { "default" },
                WorkerCount = Math.Max(2, Environment.ProcessorCount)
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                Queues = new[] { "zip" },
                WorkerCount = Math.Max(2, Environment.ProcessorCount * 2)
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                Queues = new[] { "maintenance" },
                WorkerCount = Math.Max(1, Environment.ProcessorCount / 2)
            });

            // =========================
            //     TUS configuration
            // =========================
            var tusStorePath = HostingEnvironment.MapPath("~/App_Data/Tus/Store");
            Directory.CreateDirectory(tusStorePath);
            var finalizer = new BOBDrive.Services.Tus.TusUploadFinalizer();

            app.UseTus(_ => new DefaultTusConfiguration
            {
                UrlPath = "/tus",
                Store = new TusDiskStore(tusStorePath),
                Expiration = new SlidingExpiration(TimeSpan.FromHours(24)),

                Events = new Events
                {
                    OnAuthorizeAsync = ctx =>
                    {
                        var req = ctx?.HttpContext?.Request;
                        if (req == null) return Task.CompletedTask;

                        // CORS preflight
                        if (string.Equals(req.Method, "OPTIONS", StringComparison.OrdinalIgnoreCase))
                            return Task.CompletedTask;

                        // Status probing during upload
                        if (string.Equals(req.Method, "HEAD", StringComparison.OrdinalIgnoreCase))
                        {
                            try
                            {
                                ctx.HttpContext.Response.Headers["X-Upload-State"] = "uploading";
                            }
                            catch { }
                            return Task.CompletedTask;
                        }

                        // All other methods require authentication
                        if (ctx.HttpContext?.User?.Identity?.IsAuthenticated != true)
                        {
                            ctx.FailRequest(System.Net.HttpStatusCode.Unauthorized);
                        }
                        return Task.CompletedTask;
                    },

                    OnCreateCompleteAsync = async createCtx =>
                    {
                        try
                        {
                            var httpUser = createCtx.HttpContext?.User?.Identity?.Name;
                            var meta = createCtx.Metadata;

                            string filename = meta.TryGetValue("filename", out var fn)
                                ? Safe(fn.GetString(Encoding.UTF8))
                                : null;

                            string relativePath = meta.TryGetValue("relativePath", out var rp)
                                ? Safe(rp.GetString(Encoding.UTF8))
                                : (meta.TryGetValue("relative_path", out var rp2)
                                    ? Safe(rp2.GetString(Encoding.UTF8))
                                    : null);

                            // Determine actual name used for extension check
                            string effectiveName = !string.IsNullOrWhiteSpace(filename)
                                ? filename
                                : (!string.IsNullOrWhiteSpace(relativePath)
                                    ? Path.GetFileName(relativePath)
                                    : null);

                            if (!string.IsNullOrWhiteSpace(effectiveName))
                            {
                                try
                                {
                                    var ext = UploadConfiguration.NormalizeExt(Path.GetExtension(effectiveName));
                                    var allowed = await UploadConfiguration.GetAllowedExtensionsAsync();

                                    if (allowed != null && (ext == null || !allowed.Contains(ext)))
                                    {
                                        Log.Warning(
                                            "TUS create rejected: User={User} Name={Name} Ext={Ext} not allowed.",
                                            httpUser ?? "(anonymous)", effectiveName, ext ?? "(none)");

                                        createCtx.HttpContext.Response.StatusCode =
                                            (int)System.Net.HttpStatusCode.BadRequest;
                                        createCtx.HttpContext.Response.Headers["X-Error"] =
                                            "File type is not allowed by server policy.";

                                        if (createCtx.Store is tusdotnet.Interfaces.ITusTerminationStore termStore)
                                        {
                                            try { await termStore.DeleteFileAsync(createCtx.FileId, createCtx.CancellationToken); }
                                            catch { }
                                        }
                                        return;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Log.Warning(ex,
                                        "TUS extension validation failed; allowing upload for {Name}",
                                        effectiveName);
                                }
                            }

                            long? size = null;
                            if (meta.TryGetValue("size", out var szMeta) &&
                                long.TryParse(Safe(szMeta.GetString(Encoding.UTF8)), out var parsed) &&
                                parsed > 0)
                            {
                                size = parsed;
                            }
                            if (!size.HasValue && createCtx.UploadLength > 0)
                                size = createCtx.UploadLength;

                            string fullHash = meta.TryGetValue("sha256", out var mh)
                                ? Safe(mh.GetString(Encoding.UTF8))
                                : null;

                            string sampleHash = meta.TryGetValue("sampleSha256", out var sh)
                                ? Safe(sh.GetString(Encoding.UTF8))
                                : null;

                            string resumeMode = meta.TryGetValue("resumeMode", out var rm)
                                ? Safe(rm.GetString(Encoding.UTF8))
                                : null;

                            string forceFresh = meta.TryGetValue("forceFresh", out var ff)
                                ? Safe(ff.GetString(Encoding.UTF8))
                                : null;

                            string clientUploadGuid = meta.TryGetValue("clientUploadGuid", out var cg)
                                ? Safe(cg.GetString(Encoding.UTF8))
                                : null;

                            var ip = GetRemoteIp(createCtx.OwinContext);
                            Log.Information(
                                "TUS create: User={User} IP={IP} TusId={TusId} Name={Name} Size={Size} ResumeMode={ResumeMode} ForceFresh={ForceFresh} ClientGuid={ClientGuid} FullHash={Full} SampleHash={Sample} RelPath={Rel}",
                                httpUser ?? "(anonymous)", ip, createCtx.FileId, filename, size, resumeMode,
                                forceFresh, clientUploadGuid, fullHash, sampleHash, relativePath);

                            using (var db = new CloudStorageDbContext())
                            {
                                var svc = new UploadSessionService(
                                    db,
                                    Log.Logger,
                                    LoggingConfig.AuditLogger,
                                    tusStorePath,
                                    "/tus");

                                await svc.CreateOrAttachAsync(
                                    createCtx.FileId,
                                    httpUser,
                                    filename,
                                    size,
                                    sampleHash,
                                    fullHash,
                                    relativePath,
                                    null);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex, "TUS OnCreateComplete failed");
                        }
                    },

                    OnFileCompleteAsync = async ctx =>
                    {
                        try
                        {
                            using (var db = new CloudStorageDbContext())
                            {
                                var svc = new UploadSessionService(
                                    db,
                                    Log.Logger,
                                    LoggingConfig.AuditLogger,
                                    tusStorePath,
                                    "/tus");

                                await svc.MarkCompletedAsync(ctx.FileId);
                            }

                            await finalizer.HandleAsync(ctx).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Log.Error(ex, "TUS OnFileCompleteAsync failed");
                        }
                    }
                }
            });

            // =========================
            //   Recurring Jobs
            // =========================

            // every 60 seconds
            RecurringJob.AddOrUpdate(
                "admin-stats-broadcast",
                () => BOBDrive.Hubs.AdminStatsHub.BroadcastSnapshot(),
                "*/1 * * * *", // every minute
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc });

            // Reconcile uploads (incomplete / stuck)
            RecurringJob.AddOrUpdate(
                "reconcile-uploads",
                () => RecurringJobRunner.ReconcileUploads(),
                Cron.Hourly,
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc }
            );

            // Clean up old TUS sessions
            RecurringJob.AddOrUpdate(
                "cleanup-tus-sessions-12h",
                () => RecurringJobRunner.CleanupTusSessions(),
                "0 */12 * * *",
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc }
            );

            // 90‑day bin purge via RecurringJobRunner (internally should use DeleteService.PurgeExpiredBinEntries)
            RecurringJob.AddOrUpdate(
                "purge-bin-older-than-90d",
                () => RecurringJobRunner.PurgeBin(),
                "0 2 * * *",
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc }
            );

            // Reconcile FileBlobs refcounts / orphans
            RecurringJob.AddOrUpdate(
                "reconcile-fileblobs",
                () => RecurringJobRunner.ReconcileFileBlobs(),
                "0 3 * * *",
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc }
            );

            // Disk orphan cleanup
            RecurringJob.AddOrUpdate(
                "cleanup-orphan-physicals",
                () => RecurringJobRunner.CleanupOrphans(),
                "30 3 * * *",
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc }
            );

            // =========================
            //  Legacy per‑Bin jobs
            //  (KEPT so nothing breaks)
            // =========================

            // Per-user bin retention job
            RecurringJob.AddOrUpdate(
                "user-bin-retention",
                () => BOBDrive.Services.Maintenance.BinRetentionJob.PurgeAsync(90, 500),
                Cron.Daily);

            // Department‑scoped bin retention job
            RecurringJob.AddOrUpdate(
                "dept-bin-retention",
                () => BOBDrive.Services.Maintenance.BinRetentionJob.PurgeDepartmentBinsAsync(90, 500),
                Cron.Daily);

            // Remove legacy per-upload cleanup jobs
            RemoveLegacyPerUploadCleanupJobs();
        }

        private static string Safe(string s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();

        private static string GetRemoteIp(IOwinContext owinCtx)
        {
            try
            {
                if (owinCtx != null)
                {
                    var ip = owinCtx.Request.RemoteIpAddress;
                    if (!string.IsNullOrWhiteSpace(ip)) return ip;

                    var headers = owinCtx.Request.Headers;
                    if (headers != null &&
                        headers.TryGetValue("X-Forwarded-For", out var vals) &&
                        vals.Length > 0)
                    {
                        var xff = vals[0];
                        if (!string.IsNullOrWhiteSpace(xff))
                            return xff.Split(',')[0].Trim();
                    }
                }
            }
            catch { }
            return "(unknown)";
        }

        private static void RemoveLegacyPerUploadCleanupJobs()
        {
            try
            {
                var monitoring = JobStorage.Current.GetMonitoringApi();
                int from = 0;
                const int pageSize = 500;

                while (true)
                {
                    var page = monitoring.ScheduledJobs(from, pageSize);
                    if (page == null || page.Count == 0) break;

                    foreach (var kv in page)
                    {
                        var jobId = kv.Key;
                        var dto = kv.Value;
                        var job = dto?.Job;

                        if (job?.Type?.FullName == "BOBDrive.Services.Uploads.UploadSessionCleanupJob"
                            && job.Method != null
                            && (job.Method.Name == "CleanupTusSession"
                                || job.Method.Name == "CleanupTusSessionsAsync"))
                        {
                            try { BackgroundJob.Delete(jobId); } catch { }
                        }
                    }

                    if (page.Count < pageSize) break;
                    from += page.Count;
                }
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Legacy per-upload cleanup job purge failed.");
            }
        }
    }

    // *** COMPLETELY REPLACE THE HangfireAuthorizationFilter CLASS WITH THIS ***
    /// <summary>
    /// Authorization filter for Hangfire Dashboard - restricts access to Admin users only
    /// </summary>
    public class HangfireAuthorizationFilter : IDashboardAuthorizationFilter
    {
        private static readonly TimeSpan LogCacheWindow = TimeSpan.FromMinutes(5);
        private static readonly object _lock = new object();
        private static readonly Dictionary<string, DateTime> _lastGranted =
            new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        public bool Authorize(DashboardContext context)
        {
            var owinContext = new OwinContext(context.GetOwinEnvironment());
            var user = owinContext.Authentication.User;

            if (user?.Identity?.IsAuthenticated != true)
            {
                Log.Warning("Hangfire Dashboard: Unauthorized access attempt (not authenticated)");
                return false;
            }

            var username = user.Identity.Name;

            try
            {
                using (var db = new CloudStorageDbContext())
                {
                    var dbUser = db.Users.AsNoTracking()
                        .FirstOrDefault(u => u.ExternalUserId == username);

                    if (dbUser == null)
                    {
                        Log.Warning("Hangfire Dashboard: User {Username} not found in database", username);
                        return false;
                    }

                    var isAdmin = UserRoles.IsAdmin(dbUser.Role);
                    if (!isAdmin)
                    {
                        Log.Warning(
                            "Hangfire Dashboard: User {Username} denied access (not admin, role: {Role})",
                            username,
                            dbUser.Role);
                        return false;
                    }

                    // Throttle "granted" log
                    var key = username ?? "(unknown)";
                    var now = DateTime.UtcNow;
                    bool shouldLog = false;

                    lock (_lock)
                    {
                        if (!_lastGranted.TryGetValue(key, out var last) ||
                            (now - last) > LogCacheWindow)
                        {
                            _lastGranted[key] = now;
                            shouldLog = true;
                        }
                    }

                    if (shouldLog)
                    {
                        Log.Information("Hangfire Dashboard: Admin access granted to {Username}", username);
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    "Hangfire Dashboard: Error checking authorization for user {Username}",
                    username);
                return false;
            }
        }
    }
}