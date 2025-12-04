using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BOBDrive.Models;
using Microsoft.AspNet.SignalR;

namespace BOBDrive.Services
{
    public static class SystemMetricsService
    {
        // Limit processes sent to clients (fast UI)
        private const int MaxProcessRows = 20;

        private static readonly object _sync = new object();
        private static volatile bool _running = false;
        private static Timer _timer;

        // Totals history
        private static readonly ConcurrentQueue<MetricSample> _history = new ConcurrentQueue<MetricSample>();
        private static int _historyLimit = 1200; // ~20 minutes @ 1 Hz

        // Latest per-process snapshot (public getter returns a copy)
        private static readonly object _procLock = new object();
        private static List<ProcessSample> _latestProcesses = new List<ProcessSample>();

        // Totals performance counters
        private static PerformanceCounter _cpuCounter;
        private static PerformanceCounter _diskCounter;
        private static PerformanceCounter _memPctCounter;
        private static PerformanceCounter[] _netCounters;

        // Per-process state to compute deltas
        private class ProcState
        {
            public TimeSpan LastCpu;
            public ulong LastReadBytes;
            public ulong LastWriteBytes;
            public DateTime LastTs;
            public string Name;
        }
        private static readonly Dictionary<int, ProcState> _procState = new Dictionary<int, ProcState>();

        // Non-reentrancy + throttling for background process sampling
        private static int _procSampling = 0; // 0 = idle, 1 = running
        private static DateTime _lastProcSample = DateTime.MinValue;
        private static readonly TimeSpan _procMinInterval = TimeSpan.FromSeconds(2);

        // Win32 interop for per-process IO counters
        [StructLayout(LayoutKind.Sequential)]
        private struct IO_COUNTERS
        {
            public ulong ReadOperationCount;
            public ulong WriteOperationCount;
            public ulong OtherOperationCount;
            public ulong ReadTransferCount;
            public ulong WriteTransferCount;
            public ulong OtherTransferCount;
        }

        private const int PROCESS_QUERY_LIMITED_INFORMATION = 0x1000;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr OpenProcess(int desiredAccess, bool inheritHandle, int processId);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetProcessIoCounters(IntPtr hProcess, out IO_COUNTERS ioCounters);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr hObject);

        public static void Start(int intervalMs = 1000, int historyLimit = 1200)
        {
            if (_running) return;

            lock (_sync)
            {
                if (_running) return;

                _historyLimit = Math.Max(60, historyLimit);
                InitializeCounters();

                // Prime counters (first NextValue is often 0)
                SafeNextValue(_cpuCounter);
                SafeNextValue(_diskCounter);
                SafeNextValue(_memPctCounter);
                if (_netCounters != null) foreach (var c in _netCounters) SafeNextValue(c);

                // Sample almost immediately so the UI isn't blank
                _timer = new Timer(Sample, null, /*dueTime*/ 10, /*period*/ intervalMs);
                _running = true;
            }
        }

        private static void InitializeCounters()
        {
            try { _cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", readOnly: true); }
            catch { _cpuCounter = null; }

            try { _diskCounter = new PerformanceCounter("PhysicalDisk", "% Disk Time", "_Total", readOnly: true); }
            catch { _diskCounter = null; }

            try { _memPctCounter = new PerformanceCounter("Memory", "% Committed Bytes In Use", readOnly: true); }
            catch { _memPctCounter = null; }

            try
            {
                var nic = new PerformanceCounterCategory("Network Interface");
                var instances = nic.GetInstanceNames();
                _netCounters = instances
                    .Select(n =>
                    {
                        try { return new PerformanceCounter("Network Interface", "Bytes Total/sec", n, readOnly: true); }
                        catch { return null; }
                    })
                    .Where(c => c != null)
                    .ToArray();
            }
            catch { _netCounters = Array.Empty<PerformanceCounter>(); }
        }

        private static float SafeNextValue(PerformanceCounter c)
        {
            if (c == null) return 0f;
            try { return c.NextValue(); } catch { return 0f; }
        }

        private static void Sample(object state)
        {
            var now = DateTime.UtcNow;

            // 1) Totals first: compute and broadcast immediately so UI gets data fast
            float cpu = SafeNextValue(_cpuCounter);
            float disk = SafeNextValue(_diskCounter);
            float mem = SafeNextValue(_memPctCounter);
            float netBps = 0;
            if (_netCounters != null && _netCounters.Length > 0)
            {
                foreach (var c in _netCounters) netBps += SafeNextValue(c);
            }
            float netMbps = (netBps * 8f) / 1_000_000f;

            var totalSample = new MetricSample
            {
                Timestamp = now,
                CpuPercent = Clamp01(cpu),
                MemoryPercent = Clamp01(mem),
                DiskPercent = Clamp01(disk),
                NetMbps = netMbps < 0 ? 0 : netMbps
            };

            EnqueueTotal(totalSample);
            BroadcastTotals(totalSample); // push totals immediately

            // 2) Processes: run in background and throttle to avoid blocking totals
            if ((now - _lastProcSample) >= _procMinInterval &&
                Interlocked.CompareExchange(ref _procSampling, 1, 0) == 0)
            {
                Task.Run(() =>
                {
                    try
                    {
                        var procs = CollectTopProcesses(DateTime.UtcNow);
                        _lastProcSample = DateTime.UtcNow;
                        BroadcastProcesses(procs);
                    }
                    catch
                    {
                        // swallow; next tick will try again
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _procSampling, 0);
                    }
                });
            }
        }

        // Optimized process collector:
        // - First pass computes CPU% + Memory for all processes (no IO handles).
        // - Keep only top N by CPU.
        // - Second pass collects IO for those top N only (fast).
        private static List<ProcessSample> CollectTopProcesses(DateTime now)
        {
            var list = new List<ProcessSample>();
            var elapsedSecDefault = 1.0;

            Process[] processes;
            try { processes = Process.GetProcesses(); }
            catch { processes = Array.Empty<Process>(); }

            int cpuCount = Environment.ProcessorCount;
            var livePidSet = new HashSet<int>();

            lock (_procLock)
            {
                foreach (var p in processes)
                {
                    int pid = -1;
                    try
                    {
                        pid = SafePid(p);
                        if (pid > 0) livePidSet.Add(pid);

                        var name = SafeProcessName(p);
                        var ws = SafeWorkingSet(p);
                        var cpuTotal = SafeTotalProcessorTime(p);

                        // Retrieve prior state if any
                        ProcState prev = null;
                        bool hasPrev = pid > 0 && _procState.TryGetValue(pid, out prev);

                        var lastTs = hasPrev ? prev.LastTs : now.AddSeconds(-elapsedSecDefault);
                        var elapsedSec = Math.Max(0.001, (now - lastTs).TotalSeconds);

                        double cpuPercent = 0;
                        if (hasPrev && cpuTotal >= prev.LastCpu)
                        {
                            var cpuDeltaMs = (cpuTotal - prev.LastCpu).TotalMilliseconds;
                            cpuPercent = Math.Min(100.0, Math.Max(0.0, cpuDeltaMs / (elapsedSec * 1000.0 * cpuCount) * 100.0));
                        }

                        list.Add(new ProcessSample
                        {
                            Pid = pid,
                            Name = name,
                            CpuPercent = (float)cpuPercent,
                            MemoryMB = ws / (1024.0 * 1024.0),
                            IoReadBps = 0,
                            IoWriteBps = 0,
                            IoTotalBps = 0,
                            Timestamp = now
                        });

                        // Update CPU timing state for ALL processes (no IO here)
                        if (pid > 0)
                        {
                            _procState[pid] = new ProcState
                            {
                                LastCpu = cpuTotal,
                                LastReadBytes = hasPrev ? prev.LastReadBytes : 0,
                                LastWriteBytes = hasPrev ? prev.LastWriteBytes : 0,
                                LastTs = now,
                                Name = name
                            };
                        }
                    }
                    catch
                    {
                        // ignore this process
                    }
                    finally
                    {
                        try { p.Dispose(); } catch { }
                    }
                }

                // Remove stale PIDs from state dictionary
                var stale = _procState.Keys.Where(k => !livePidSet.Contains(k)).ToList();
                foreach (var k in stale) _procState.Remove(k);

                // Keep only top N by CPU
                var top = list
                    .OrderByDescending(s => s.CpuPercent)
                    .ThenByDescending(s => s.MemoryMB)
                    .Take(MaxProcessRows)
                    .ToList();

                // Second pass: fetch IO for those top only
                foreach (var s in top)
                {
                    try
                    {
                        ulong readBytes = 0, writeBytes = 0;
                        GetIoCountersForPid(s.Pid, out readBytes, out writeBytes);

                        // Compute deltas against stored state
                        ProcState prev = null;
                        bool hasPrev = s.Pid > 0 && _procState.TryGetValue(s.Pid, out prev);
                        var lastTs = hasPrev ? prev.LastTs : now.AddSeconds(-elapsedSecDefault);
                        var elapsedSec = Math.Max(0.001, (now - lastTs).TotalSeconds);

                        double readBps = 0, writeBps = 0;
                        if (hasPrev)
                        {
                            var rDelta = readBytes >= prev.LastReadBytes ? (readBytes - prev.LastReadBytes) : 0UL;
                            var wDelta = writeBytes >= prev.LastWriteBytes ? (writeBytes - prev.LastWriteBytes) : 0UL;
                            readBps = rDelta / elapsedSec;
                            writeBps = wDelta / elapsedSec;
                        }

                        s.IoReadBps = readBps;
                        s.IoWriteBps = writeBps;
                        s.IoTotalBps = readBps + writeBps;

                        // Update IO counters in state for these top processes
                        if (s.Pid > 0)
                        {
                            _procState[s.Pid] = new ProcState
                            {
                                LastCpu = hasPrev ? prev.LastCpu : TimeSpan.Zero, // we already updated LastCpu/LastTs above; preserve if possible
                                LastReadBytes = readBytes,
                                LastWriteBytes = writeBytes,
                                LastTs = now,
                                Name = s.Name
                            };
                        }
                    }
                    catch
                    {
                        // ignore IO failures for individual processes
                    }
                }

                // Final sort: CPU desc, then IO desc
                top = top
                    .OrderByDescending(s => s.CpuPercent)
                    .ThenByDescending(s => s.IoTotalBps)
                    .ToList();

                _latestProcesses = top;
                return top;
            }
        }

        private static string SafeProcessName(Process p)
        {
            try { return string.IsNullOrEmpty(p.ProcessName) ? ("pid:" + p.Id) : p.ProcessName; }
            catch { return "pid:" + SafePid(p); }
        }

        private static int SafePid(Process p)
        {
            try { return p.Id; } catch { return -1; }
        }

        private static TimeSpan SafeTotalProcessorTime(Process p)
        {
            try { return p.TotalProcessorTime; } catch { return TimeSpan.Zero; }
        }

        private static long SafeWorkingSet(Process p)
        {
            try { return p.WorkingSet64; } catch { return 0L; }
        }

        private static void GetIoCountersForPid(int pid, out ulong readBytes, out ulong writeBytes)
        {
            readBytes = 0; writeBytes = 0;
            if (pid <= 0) return;

            IntPtr h = IntPtr.Zero;
            try
            {
                h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid);
                if (h == IntPtr.Zero) return;

                if (GetProcessIoCounters(h, out IO_COUNTERS counters))
                {
                    readBytes = counters.ReadTransferCount;
                    writeBytes = counters.WriteTransferCount;
                }
            }
            catch { }
            finally
            {
                if (h != IntPtr.Zero) try { CloseHandle(h); } catch { }
            }
        }

        private static float Clamp01(float v)
        {
            if (float.IsNaN(v) || float.IsInfinity(v)) return 0f;
            if (v < 0f) return 0f;
            if (v > 100f) return 100f;
            return v;
        }

        private static void EnqueueTotal(MetricSample sample)
        {
            _history.Enqueue(sample);
            while (_history.Count > _historyLimit && _history.TryDequeue(out _)) { }
        }

        private static void BroadcastTotals(MetricSample totals)
        {
            try
            {
                var ctx = GlobalHost.ConnectionManager.GetHubContext<BOBDrive.Hubs.ServerMetricsHub>();
                ctx.Clients.All.updateTotals(totals);
            }
            catch
            {
                // Ignore if SignalR not initialized
            }
        }

        private static void BroadcastProcesses(List<ProcessSample> processes)
        {
            try
            {
                var ctx = GlobalHost.ConnectionManager.GetHubContext<BOBDrive.Hubs.ServerMetricsHub>();
                ctx.Clients.All.updateProcesses(processes);
            }
            catch
            {
                // Ignore if SignalR not initialized
            }
        }

        public static IList<MetricSample> GetHistory(int maxPoints)
        {
            maxPoints = Math.Max(1, Math.Min(maxPoints, _historyLimit));
            var arr = _history.ToArray();
            if (arr.Length <= maxPoints) return arr.ToList();
            return arr.Skip(arr.Length - maxPoints).ToList();
        }

        public static IList<ProcessSample> GetLatestProcesses(int max = MaxProcessRows)
        {
            lock (_procLock)
            {
                return _latestProcesses.Take(Math.Max(1, max)).ToList();
            }
        }
    }
}