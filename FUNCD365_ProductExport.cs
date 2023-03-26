using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using VSI.CloudPlaform.Core.Db;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Core.Telemetry;
using VSI.CloudPlatform.Model.Jobs;

namespace FunctionApp.RCK.D365.ProductExport
{
    public static partial class FUNC_RCK_D365_ProductExport
    {
        private static string storageConnection = Environment.GetEnvironmentVariable("CommonStorageConnetionString");
        private static string instrumentationKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static bool excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);
        private static int cacheExpire = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("CacheTimeout"), 60);
        private static TenantConfigModel tenantConfig;

        [FunctionName("FUNC_RCK_D365_ProductExport")]
        public static async Task<HttpResponseMessage> FUNC_D365HttpReceive([HttpTrigger(AuthorizationLevel.Function, "POST")] HttpRequestMessage req)
        {
            TelemetryClient telemetryClient = null;
            IOperationHolder<RequestTelemetry> operation = null;

            var request = await req.Content.ReadAsStringAsync();
            var functionParams = JsonConvert.DeserializeObject<FunctionParams>(request);

            try
            {
                var instanceKey = $"{functionParams.TransactionName}_{functionParams.PartnerShipId}_{functionParams.TransactionStep}";

                telemetryClient = TelemetryFactory.GetInstance($"{instanceKey}", instrumentationKey, excludeDependency);

                operation = telemetryClient.StartOperation<RequestTelemetry>(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, Guid.NewGuid().ToString());

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, "Starting...");

                tenantConfig = TenantTableHelper.GetTenantModel(storageConnection, functionParams.TenantId, cacheExpire, telemetryClient);

                if (tenantConfig == null)
                {
                    throw new Exception($"Tenant {functionParams.TenantId} is not configured");
                }

                if (functionParams.partnerShip == null && !string.IsNullOrEmpty(functionParams.PartnerShipId))
                {
                    functionParams.partnerShip = CommonUtility.GetPartnerShip(tenantConfig.CloudDb, functionParams.PartnerShipId);
                }

                if (functionParams.partnerShip == null)
                {
                    throw new Exception("Partnership object is empty!");
                }
               

                await FunctionHelper.ProcessAsync(functionParams, telemetryClient, tenantConfig);

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, "Finished.");

                return new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                var depthException = CommonUtility.GetDepthInnerException(ex);

                telemetryClient.TrackException(depthException);

                throw;
            }
            finally
            {
                telemetryClient.StopOperation(operation);
            }
        }
    }
}