using System.Threading.Tasks;

namespace BOBDrive.Services.FileOps
{
    public interface IPasteService
    {
        Task<string> StartAsync(
            string externalUserId,
            string mode,
            int destinationFolderId,
            int[] fileIds,
            int[] folderIds);

        Task ProcessAsync(
            string opId,
            string externalUserId,
            string mode,
            int destinationFolderId,
            int[] fileIds,
            int[] folderIds);
    }
}