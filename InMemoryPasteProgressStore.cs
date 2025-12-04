using Serilog;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    public class InMemoryPasteProgressStore : IPasteProgressStore
    {
        private static readonly Lazy<InMemoryPasteProgressStore> _lazy = new Lazy<InMemoryPasteProgressStore>(() => new InMemoryPasteProgressStore());
        public static InMemoryPasteProgressStore Instance => _lazy.Value;

        private readonly ConcurrentDictionary<string, PasteProgressState> _ops = new ConcurrentDictionary<string, PasteProgressState>(StringComparer.OrdinalIgnoreCase);
        private readonly ILogger _log = Log.ForContext<InMemoryPasteProgressStore>();

        public void Init(string opId, string mode, int destinationFolderId, string destinationDisplayPath)
        {
            _ops[opId] = new PasteProgressState
            {
                OpId = opId,
                Mode = mode,
                DestinationFolderId = destinationFolderId,
                DestinationDisplayPath = destinationDisplayPath,
                Stage = "Preparing",
                TotalCount = 0,
                ProcessedCount = 0
            };
        }

        public bool TryGet(string opId, out PasteProgressState state) => _ops.TryGetValue(opId, out state);

        public void Update(string opId, Action<PasteProgressState> update)
        {
            if (_ops.TryGetValue(opId, out var s))
            {
                update?.Invoke(s);
            }
        }

        public void Complete(string opId)
        {
            Update(opId, s =>
            {
                s.Stage = "Completed";
                s.IsDone = true;
            });
        }

        public void Fail(string opId, string error)
        {
            Update(opId, s =>
            {
                s.Stage = "Failed";
                s.IsDone = true;
                s.ErrorMessage = error;
            });
        }

        public void ScheduleCleanup(string opId, TimeSpan keepFor)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(keepFor);
                    _ops.TryRemove(opId, out _);
                }
                catch (Exception ex)
                {
                    _log.Warning(ex, "Cleanup timer failed for op {OpId}", opId);
                }
            });
        }
    }
}