using Serilog;
using System;
using System.IO;
using System.Web.Hosting;
// LOG LINE
using Serilog.Enrichers;
using Serilog.Core;

namespace BOBDrive.App_Start
{
    public static class LoggingConfig
    {
        public static ILogger AuditLogger { get; private set; }

        // NOTE: Log viewer assumes logs live under: App_Data/Logs/
        // - upload-log-*.txt
        // - audit-sharelink-log-*.txt

        public static void Configure()
        {
            var appDataPath = HostingEnvironment.MapPath("~/App_Data");
            if (appDataPath == null)
            {
                appDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data");
            }
            var logPath = Path.Combine(appDataPath, "Logs", "upload-log-.txt");

            // PATH FOR THE AUDIT LOG
            var auditLogPath = Path.Combine(appDataPath, "Logs", "audit-sharelink-log-.txt");

            Directory.CreateDirectory(Path.GetDirectoryName(logPath));

            // DIRECTORY CREATION FOR THE AUDIT LOG
            Directory.CreateDirectory(Path.GetDirectoryName(auditLogPath));

            // LoggerConfiguration chain
            Log.Logger = new LoggerConfiguration()
               .MinimumLevel.Debug()
               .Enrich.FromLogContext()
               .Enrich.WithThreadId()
               .Enrich.WithProcessId()
               .Enrich.WithMachineName()
               .WriteTo.File(
                   path: logPath,
                   outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] (T:{ThreadId}/P:{ProcessId}) {Message:lj} {NewLine}{Exception}{Properties:j}{NewLine}",
                   rollingInterval: RollingInterval.Day,
                   retainedFileCountLimit: 90,
                   fileSizeLimitBytes: 1024 * 1024 * 1024,
                   rollOnFileSizeLimit: true,
                   shared: true,
                   flushToDiskInterval: TimeSpan.FromSeconds(1)
               )
               .CreateLogger();


            // ENTIRE BLOCK TO CONFIGURE THE NEW AUDIT LOGGER
            AuditLogger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .Enrich.FromLogContext()
                .WriteTo.File(
                    path: auditLogPath,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [AUDIT] {Message:lj}{NewLine}{Properties:j}",
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: 180, // Keep audit logs for longer
                    shared: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(2)
                )
                .CreateLogger();

            Log.Information("Logging system initialized successfully. Log path: {LogPath}", logPath);
            Log.Information("Audit logging initialized successfully. Log path: {AuditLogPath}", auditLogPath);
        }
    }
}