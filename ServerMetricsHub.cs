using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using BOBDrive.Services;

namespace BOBDrive.Hubs
{
    public class ServerMetricsHub : Hub
    {
        public override Task OnConnected()
        {
            // Bootstrap with last 60 total samples + current top processes
            var history = SystemMetricsService.GetHistory(60);
            var procs = SystemMetricsService.GetLatestProcesses(15);
            Clients.Caller.bootstrap(history, procs);
            // ClientActivityMonitor will push updateClientActivity shortly via timer
            return base.OnConnected();
        }

        public void RequestHistory(int points = 60)
        {
            var history = SystemMetricsService.GetHistory(points);
            Clients.Caller.bootstrap(history, SystemMetricsService.GetLatestProcesses(15));
        }
    }
}