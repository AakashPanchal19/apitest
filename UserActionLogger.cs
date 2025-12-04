using Serilog;

namespace BOBDrive.Services.Logging
{
    public static class UserActionLogger
    {
        public static void LogStep(string userId, string ip, string operationId, string action, string target, string outcome, bool error = false, string errorMessage = null)
        {
            var log = Log.ForContext("UserId", userId)
                         .ForContext("ClientIP", ip)
                         .ForContext("OperationId", operationId)
                         .ForContext("Action", action)
                         .ForContext("Target", target);

            if (error)
            {
                log.Error("Outcome=FAIL Message={ErrorMessage} Action={Action} Target={Target}", errorMessage, action, target);
            }
            else
            {
                log.Information("Outcome=OK Action={Action} Target={Target} Details={Outcome}", action, target, outcome);
            }
        }
    }
}