using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Microsoft.AspNet.SignalR;
using BOBDrive.Hubs;

namespace BOBDrive.Services
{
    public class ClientStats
    {
        public string IpAddress { get; set; }
        public DateTime LastActive { get; set; }

        // Active, in-flight counters (NOT cumulative)
        public int ActiveTusSessions;
        public int ActiveZipJobs;
        public int ActiveFileOps;

        // Bytes being processed in current finalizations
        public long ActiveBytesFinalizing;

        public int TotalActive =>
            Math.Max(0, ActiveTusSessions) +
            Math.Max(0, ActiveZipJobs) +
            Math.Max(0, ActiveFileOps);

        public double ResourceScore =>
            (ActiveZipJobs * 50.0) +
            (ActiveTusSessions * 10.0) +
            (ActiveFileOps * 5.0) +
            (ActiveBytesFinalizing / (1024.0 * 1024.0));
    }

    /// <summary>
    /// Tracks active operations per IP and broadcasts top IPs to ServerMetricsHub.
    /// When all active counters for an IP reach 0, it is removed.
    /// </summary>
    public static class ClientActivityMonitor
    {
        private static readonly ConcurrentDictionary<string, ClientStats> _clients =
            new ConcurrentDictionary<string, ClientStats>(StringComparer.OrdinalIgnoreCase);

        private static readonly Timer _broadcastTimer;

        static ClientActivityMonitor()
        {
            _broadcastTimer = new Timer(BroadcastLoop, null, 2000, 2000);
        }

        private static string NormalizeIp(string ip)
        {
            if (string.IsNullOrWhiteSpace(ip)) return "(unknown)";
            ip = ip.Trim();

            // Simple IPv4:port case
            var colonIdx = ip.IndexOf(':');
            if (colonIdx > 0 && ip.Count(c => c == ':') == 1)
                ip = ip.Substring(0, colonIdx);

            if (ip == "::1") ip = "127.0.0.1";
            return ip;
        }

        private static ClientStats GetOrAdd(string rawIp)
        {
            var ip = NormalizeIp(rawIp);
            return _clients.GetOrAdd(ip, k => new ClientStats
            {
                IpAddress = k,
                LastActive = DateTime.UtcNow
            });
        }

        // ===== TUS upload sessions =====

        public static void OnTusSessionStarted(string ip)
        {
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Increment(ref stats.ActiveTusSessions);
        }

        public static void OnTusSessionCompleted(string ip)
        {
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Decrement(ref stats.ActiveTusSessions);
            CleanupIfIdle(stats);
        }

        // ===== Finalization bytes =====

        public static void OnFinalizationBytesStarted(string ip, long bytes)
        {
            if (bytes <= 0) return;
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Add(ref stats.ActiveBytesFinalizing, bytes);
        }

        public static void OnFinalizationBytesCompleted(string ip, long bytes)
        {
            if (bytes <= 0) return;
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Add(ref stats.ActiveBytesFinalizing, -bytes);
            CleanupIfIdle(stats);
        }

        // ===== ZIP jobs =====

        public static void OnZipStarted(string ip)
        {
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Increment(ref stats.ActiveZipJobs);
        }

        public static void OnZipCompleted(string ip)
        {
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Decrement(ref stats.ActiveZipJobs);
            CleanupIfIdle(stats);
        }

        // ===== File operations (copy/paste/delete) =====

        public static void OnFileOpsStarted(string ip, int count = 1)
        {
            if (count <= 0) return;
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Add(ref stats.ActiveFileOps, count);
        }

        public static void OnFileOpsCompleted(string ip, int count = 1)
        {
            if (count <= 0) return;
            var stats = GetOrAdd(ip);
            stats.LastActive = DateTime.UtcNow;
            Interlocked.Add(ref stats.ActiveFileOps, -count);
            CleanupIfIdle(stats);
        }

        // ===== Helpers =====

        private static void CleanupIfIdle(ClientStats stats)
        {
            if (stats.TotalActive <= 0 && stats.ActiveBytesFinalizing <= 0)
            {
                _clients.TryRemove(stats.IpAddress, out _);
            }
        }

        private static void BroadcastLoop(object _)
        {
            try
            {
                var snapshot = _clients.Values
                    .Where(c => c.TotalActive > 0 || c.ActiveBytesFinalizing > 0)
                    .Select(c => new
                    {
                        c.IpAddress,
                        c.LastActive,
                        TusSessionStarts = Math.Max(0, c.ActiveTusSessions),
                        ZipRequests = Math.Max(0, c.ActiveZipJobs),
                        FileOperations = Math.Max(0, c.ActiveFileOps),
                        BytesFinalized = Math.Max(0L, c.ActiveBytesFinalizing),
                        c.ResourceScore
                    })
                    .OrderByDescending(c => c.ResourceScore)
                    .Take(20)
                    .ToList();

                var hub = GlobalHost.ConnectionManager.GetHubContext<ServerMetricsHub>();
                hub.Clients.All.updateClientActivity(snapshot);
            }
            catch
            {
                // best-effort
            }
        }
    }
}