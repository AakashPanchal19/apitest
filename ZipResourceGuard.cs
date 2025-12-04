using System;
using System.Configuration;
using System.IO;
using System.Linq;
using BOBDrive.Services; // for SystemMetricsService
using BOBDrive.App_Start;
using Serilog;

namespace BOBDrive.Services
{
    /// <summary>
    /// Central resource guard to decide whether zipping work should be deferred.
    /// Uses recent SystemMetricsService samples and disk free space thresholds.
    /// Thresholds are configurable via Web.config appSettings.
    /// </summary>
    public static class ZipResourceGuard
    {
        private static readonly ILogger _log = Log.ForContext(typeof(ZipResourceGuard));

        private static double GetDouble(string key, double def)
        {
            var s = ConfigurationManager.AppSettings[key];
            return double.TryParse(s, out var v) ? v : def;
        }

        private static long GetLong(string key, long def)
        {
            var s = ConfigurationManager.AppSettings[key];
            return long.TryParse(s, out var v) ? v : def;
        }

        /// <summary>
        /// Returns true if zipping should be deferred due to high resource utilization.
        /// Sets a human readable reason explaining what threshold is exceeded.
        /// Logs the decision, thresholds and live metrics to Serilog.
        /// </summary>
        public static bool ShouldDefer(out string reason)
        {
            reason = null;

            // Read thresholds (defaults are conservative)
            var cpuMax = GetDouble("ZipGateMaxCpuPercent", 75);    // %
            var memMax = GetDouble("ZipGateMaxMemoryPercent", 80); // %
            var diskMax = GetDouble("ZipGateMaxDiskPercent", 75);  // %
            var minFreeGb = GetLong("ZipGateMinFreeDiskGB", 5);    // GB

            // Pull last few samples to smooth
            var history = SystemMetricsService.GetHistory(5);
            if (history == null || history.Count == 0)
            {
                _log.Warning("ZIP_GUARD: No metrics history available; allowing by default.");
                return false;
            }

            double avgCpu = history.Average(s => (double)s.CpuPercent);
            double avgMem = history.Average(s => (double)s.MemoryPercent);
            double avgDisk = history.Average(s => (double)s.DiskPercent);

            long freeGbFinal = long.MinValue;
            long freeGbMerge = long.MinValue;

            try
            {
                string finalRoot = Path.GetPathRoot(UploadConfiguration.FinalUploadPath);
                string mergeRoot = Path.GetPathRoot(UploadConfiguration.TempMergePath);

                freeGbFinal = GetFreeGb(finalRoot);
                freeGbMerge = GetFreeGb(mergeRoot);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "ZIP_GUARD: Failed to compute free disk space; proceeding with utilization-only gating.");
            }

            // Decision and logging (one exit per branch with full context)
            if (avgCpu >= cpuMax)
            {
                reason = $"High CPU load ({avgCpu:0}% ≥ {cpuMax:0}% threshold)";
                _log.Information("ZIP_GUARD decision=DEFER reason='{Reason}' cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                    reason, avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
                return true;
            }
            if (avgMem >= memMax)
            {
                reason = $"High memory usage ({avgMem:0}% ≥ {memMax:0}% threshold)";
                _log.Information("ZIP_GUARD decision=DEFER reason='{Reason}' cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                    reason, avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
                return true;
            }
            if (avgDisk >= diskMax)
            {
                reason = $"High disk activity ({avgDisk:0}% ≥ {diskMax:0}% threshold)";
                _log.Information("ZIP_GUARD decision=DEFER reason='{Reason}' cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                    reason, avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
                return true;
            }

            if (freeGbFinal != long.MinValue && freeGbFinal < minFreeGb)
            {
                reason = $"Low free space on final drive ({freeGbFinal} GB < {minFreeGb} GB)";
                _log.Information("ZIP_GUARD decision=DEFER reason='{Reason}' cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                    reason, avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
                return true;
            }
            if (freeGbMerge != long.MinValue && freeGbMerge < minFreeGb)
            {
                reason = $"Low free space on temp/merge drive ({freeGbMerge} GB < {minFreeGb} GB)";
                _log.Information("ZIP_GUARD decision=DEFER reason='{Reason}' cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                    reason, avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
                return true;
            }

            // Allow
            _log.Information("ZIP_GUARD decision=ALLOW cpu={Cpu:0.0}%/{CpuMax:0.0}% mem={Mem:0.0}%/{MemMax:0.0}% disk={Disk:0.0}%/{DiskMax:0.0}% free_final_gb={FreeFinal} free_merge_gb={FreeMerge} min_free_gb={MinFree}",
                avgCpu, cpuMax, avgMem, memMax, avgDisk, diskMax, freeGbFinal, freeGbMerge, minFreeGb);
            return false;
        }

        private static long GetFreeGb(string root)
        {
            if (string.IsNullOrWhiteSpace(root)) return long.MaxValue;
            var di = new DriveInfo(root);
            return di.AvailableFreeSpace / (1024L * 1024L * 1024L);
        }
    }
}