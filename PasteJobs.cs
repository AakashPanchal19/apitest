using Hangfire;
using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    public static class PasteJobs
    {
        [Queue("default")]
        public static async Task Process(string opId, string externalUserId, string mode, int destinationFolderId, int[] fileIds, int[] folderIds)
        {
            var svc = new PasteService(InMemoryPasteProgressStore.Instance);
            await svc.ProcessAsync(opId, externalUserId, mode, destinationFolderId, fileIds, folderIds);
        }
    }
}