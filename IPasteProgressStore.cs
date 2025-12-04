using System;

namespace BOBDrive.Services.FileOps
{
    public interface IPasteProgressStore
    {
        void Init(string opId, string mode, int destinationFolderId, string destinationDisplayPath);
        bool TryGet(string opId, out PasteProgressState state);
        void Update(string opId, Action<PasteProgressState> update);
        void Complete(string opId);
        void Fail(string opId, string error);
        void ScheduleCleanup(string opId, TimeSpan keepFor);
    }
}