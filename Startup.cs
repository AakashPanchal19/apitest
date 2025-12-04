using Owin;
using Microsoft.AspNet.SignalR; // ADD THIS

[assembly: Microsoft.Owin.OwinStartup(typeof(BOBDrive.Startup))]

namespace BOBDrive
{
    // Thin wrapper to ensure OWIN startup runs and reuses your HangfireConfig
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            // Map SignalR so ~/signalr/hubs is available and hubs (e.g., AdminStatsHub) can push updates
            app.MapSignalR(); // ADD THIS LINE

            // Keep existing Hangfire configuration
            new BOBDrive.App_Start.HangfireConfig().Configuration(app);
        }
    }
}