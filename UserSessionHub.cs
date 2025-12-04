using Microsoft.AspNet.SignalR;
using Serilog;
using System;
using System.Threading.Tasks;

namespace BOBDrive.Hubs
{
    /// <summary>
    /// Hub used to:
    ///  - Map connectionId -> ExternalUserId
    ///  - Force logout of a specific user on all open browser tabs
    /// </summary>
    [Authorize]
    public class UserSessionHub : Hub
    {
        private static readonly ILogger _log = Log.ForContext<UserSessionHub>();

        public override Task OnConnected()
        {
            try
            {
                var externalId = Context.User?.Identity?.Name ?? "(unknown)";
                // Track mapping in memory so we can target this user later if needed.
                // For now we just log it; if you want per‑user groups, you can also do:
                // Groups.Add(Context.ConnectionId, "user-" + externalId);
                _log.Information("UserSessionHub connected: ConnId={ConnectionId}, User={User}",
                    Context.ConnectionId, externalId);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Error during UserSessionHub.OnConnected");
            }
            return base.OnConnected();
        }

        public override Task OnDisconnected(bool stopCalled)
        {
            try
            {
                var externalId = Context.User?.Identity?.Name ?? "(unknown)";
                _log.Information("UserSessionHub disconnected: ConnId={ConnectionId}, User={User}, StopCalled={StopCalled}",
                    Context.ConnectionId, externalId, stopCalled);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Error during UserSessionHub.OnDisconnected");
            }
            return base.OnDisconnected(stopCalled);
        }

        /// <summary>
        /// Called from server: forces all clients for the given external user ID to log out.
        /// </summary>
        public static void ForceLogoutUser(string externalUserId, string reason)
        {
            var context = GlobalHost.ConnectionManager.GetHubContext<UserSessionHub>();
            if (context == null) return;

            try
            {
                // Use a group name per user for targeting; clients join this group in js.
                var group = "user-" + (externalUserId ?? "").Trim();
                context.Clients.Group(group).forceLogout(reason ?? "Your department membership has been changed.");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error broadcasting force logout for user {UserId}", externalUserId);
            }
        }

        /// <summary>
        /// Client calls this after connecting so we can add them to their own user group.
        /// </summary>
        public Task RegisterUserConnection(string externalUserId)
        {
            if (string.IsNullOrWhiteSpace(externalUserId))
                externalUserId = Context.User?.Identity?.Name;

            if (string.IsNullOrWhiteSpace(externalUserId))
                return Task.FromResult(0);

            var group = "user-" + externalUserId.Trim();
            _log.Information("UserSessionHub.RegisterUserConnection: ConnId={Conn}, User={User}, Group={Group}",
                Context.ConnectionId, externalUserId, group);
            return Groups.Add(Context.ConnectionId, group);
        }
    }
}