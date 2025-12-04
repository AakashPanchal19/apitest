using Hangfire;
using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    public static class DeleteJobs
    {
        [Queue("default")]
        public static async Task Process(string opId, string externalUserId, string requesterIp, int[] fileIds, int[] folderIds)
        {
            await DeleteService.ProcessAsync(opId, externalUserId, requesterIp, fileIds, folderIds);
        }

        [Queue("default")]
        public static async Task EmptyBin(string opId, string externalUserId, string requesterIp)
        {
            await DeleteService.EmptyBinAsync(opId, externalUserId, requesterIp);
        }

        [Queue("default")]
        public static async Task HardDeleteDirect(string opId, string requestedBy, string requesterIp, int[] fileIds, int[] folderIds)
        {
            await DeleteService.HardDeleteDirectAsync(opId, requestedBy, requesterIp, fileIds, folderIds);
        }
    }
}