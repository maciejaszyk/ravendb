using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsRequestAuthorizer : AbstractRequestAuthorizer
	{
		private readonly List<string> requiredGroups = new List<string>();
		private readonly List<string> requiredUsers = new List<string>();

		protected override void Initialize()
		{
			var doc = server.SystemDatabase.Get("Raven/Authorization/WindowsSettings", null);
			requiredGroups.Clear();
			requiredUsers.Clear();

			var required = doc.DataAsJson.JsonDeserialization<WindowsAuthDocument>();
			if (required != null)
			{
				requiredGroups.AddRange(required.RequiredGroups);
				requiredUsers.AddRange(required.RequiredUsers);
			}
		}

		public override bool Authorize(IHttpContext ctx)
		{
			Action onRejectingRequest;
			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None && IsInvalidUser(ctx, out onRejectingRequest))
			{
				onRejectingRequest();
				return false;
			}

			var httpRequest = ctx.Request;

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				IsInvalidUser(ctx, out onRejectingRequest) &&
				IsGetRequest(httpRequest.HttpMethod, httpRequest.Url.AbsolutePath) == false)
			{
				onRejectingRequest();
				return false;
			}

			if (IsInvalidUser(ctx, out onRejectingRequest) == false)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = ctx.User.Identity.Name;
				CurrentOperationContext.User.Value = ctx.User;
			}
			return true;
		}

		private bool IsInvalidUser(IHttpContext ctx, out Action onRejectingRequest)
		{
			var invalidUser = (ctx.User == null ||
							   ctx.User.Identity.IsAuthenticated == false);
			if (invalidUser)
			{
				onRejectingRequest = ctx.SetStatusToForbidden;
				return true;
			}


			onRejectingRequest = ctx.SetStatusToUnauthorized;

			if (requiredGroups.Count > 0 || requiredUsers.Count > 0)
			{
			
				if (requiredGroups.Any(requiredGroup => ctx.User.IsInRole(requiredGroup)) ||
					requiredUsers.Any(requiredUser => string.Equals(ctx.User.Identity.Name, requiredUser, StringComparison.InvariantCultureIgnoreCase)))
					return false;

				return true;
			}
			
			return false;
		}
	}
}