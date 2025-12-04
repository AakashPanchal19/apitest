// File: App_Start/AdHelper.cs (new)
using System;
using System.DirectoryServices.AccountManagement;
using BOBDrive.App_Start;

namespace BOBDrive.Infrastructure
{
    public static class AdHelper
    {
        public static bool UserExists(string samAccountName)
        {
            if (string.IsNullOrWhiteSpace(samAccountName))
                return false;

            try
            {
                using (var context = new PrincipalContext(
                    ContextType.Domain,
                    UploadConfiguration.ActiveDirectoryDomain))
                using (var user = UserPrincipal.FindByIdentity(
                    context,
                    IdentityType.SamAccountName,
                    samAccountName))
                {
                    return user != null;
                }
            }
            catch
            {
                return false;
            }
        }
    }
}