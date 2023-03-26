using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.Model.Transport;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using VSI.CloudPlaform.Core.Db;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Core.Common;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Model.Common;
using VSI.CloudPlatform.Model.Jobs;
using VSI.Common;
using RCK.CloudPlatform.Model.D365;
using RCK.CloudPlatform.Model.Product;
using RCK.CloudPlatform.Model;
using System.Collections.Generic;

namespace FunctionApp.RCK.D365.ProductExport
{
    public class FunctionHelper
    {
        public static async Task ProcessAsync(FunctionParams functionParams, TelemetryClient telemetryClient, TenantConfigModel tenantConfig)
        {
            var cacheExpireInMinutes = FunctionUtilities.GetIntValue(Environment.GetEnvironmentVariable("D365ConfigCacheExpireInMinutes"), 120);           
            var startDate = DateTime.Now;

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, "Extracting last run value...");

            var requestParams = GetProductExportRequestParams(functionParams, tenantConfig, telemetryClient, cacheExpireInMinutes);

            var lastRunHistory = CommonUtility.GetLastRunHistory(tenantConfig.CloudDb, functionParams);

            var accessToken = await GetClientCredentialsAccessTokenAsync(requestParams.Security);

            // Fetching changed products ids

            long lastRowversion = 0;
            if (lastRunHistory != null && !string.IsNullOrWhiteSpace(lastRunHistory?.key_value) && lastRunHistory?.key_value?.ToLower() != "null")
            {
                lastRowversion = Convert.ToInt64(lastRunHistory.key_value);
            }

            var changedProductsRequestBody = "{\"request\":{\"LastRowversion\":" + lastRowversion + "}}";

            var (changedProductsSuccess, changedProductsErrorMessage, changedProductsResponseString) = await GetServiceDataAsync(requestParams, telemetryClient, requestParams.GetChangedProductApiUri, changedProductsRequestBody, accessToken);

            if (changedProductsSuccess)
            {
                var cpResponse = JsonConvert.DeserializeObject<ChangedProductResponse>(changedProductsResponseString);

                // Fetching Products details


                var productBatches = cpResponse.Products.Batches(requestParams.BatchSize);
                List<string> productsFromD365 = new List<string>();
                Parallel.ForEach(productBatches, new ParallelOptions { MaxDegreeOfParallelism = requestParams.NoOfThreads }, (productBatch) =>
                {
                    var productDetailsRequest = requestParams.ProductDetailsAPIRequestBody;

                    productDetailsRequest.Products = productBatch;

                    var productDetailsRequestBody = JsonConvert.SerializeObject(productDetailsRequest);

                    productDetailsRequestBody = "{\"request\": " + productDetailsRequestBody + "}";

                    var (productDetailsSuccess, productDetailsErrorMessage, productDetailsResponseString) = GetServiceDataAsync(requestParams, telemetryClient, requestParams.GetProductDetailsApiUri, productDetailsRequestBody, accessToken).GetAwaiter().GetResult();

                    if (productDetailsSuccess)
                    {


                        productsFromD365.Add(productDetailsResponseString);
                        //ProcessDataAsync(telemetryClient, functionParams, startDate, productDetailsResponseString, tenantConfig, requestParams, lastRunHistory, cpResponse.MaxRowversion).GetAwaiter().GetResult();
                    }
                    else
                    {
                        throw new Exception(productDetailsErrorMessage);
                    }
                });
                List<Products> finalProducts = new List<Products>();
                long count = 0;
                string oData = null;
                bool Success = false;
                dynamic Errors = null;
                foreach (var batchProducts in productsFromD365)
                {                   
                    var products = JsonConvert.DeserializeObject<ProductMapping>(batchProducts);
                    finalProducts.AddRange(products.Products);                                                           
                    count += products.TotalCount;
                    oData = products.odatacontext;
                    Success = products.Success;
                    Errors = products.Errors;

                }
                object completeProduct = new
                {

                    @odatacontext = oData,
                    Products = finalProducts,
                    TotalCount = count,
                    Success = Success,
                    Errors = Errors,
                    MaxRowVersion = cpResponse.MaxRowversion.ToString()

                };
                var completeProducts = JsonConvert.SerializeObject(completeProduct);
                
                ProcessDataAsync(telemetryClient, functionParams, startDate, completeProducts, tenantConfig, requestParams, lastRunHistory, cpResponse.MaxRowversion).GetAwaiter().GetResult();
                CommonUtility.UpdateLastRunHistory(tenantConfig.CloudDb, functionParams, lastRunHistory, cpResponse.MaxRowversion.ToString());

            }
                else
                {
                    throw new Exception(changedProductsErrorMessage);
                }
            }

        private async static Task<string> ProcessDataAsync(TelemetryClient telemetryClient, FunctionParams functionParams, DateTime startDate, string fileData, TenantConfigModel tenant, ProductExportRequestParams requestParams, ExtractHistory lastRunHistory, long maxRowversion)
        {
            var archiveContainer = Environment.GetEnvironmentVariable("BlobArchiveContainer");

            var transactionId = Utilities.GetTransactionId();

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, $"TransactionId is {transactionId}");

            var filePath = transactionId;
            var path = $"{functionParams.partnerShip.Partnership_Code}/{transactionId}/{functionParams.TransactionStep}_{filePath}";
            var fileBlobPath = await tenant.AzureBlob.UploadFileToBlobStorageAsync(fileData, path, archiveContainer);

            dynamic data = new JObject();
            dynamic serviceBusConnection = new JObject();

            serviceBusConnection.ServiceBusConnection = tenant.ServicebusConnectionString;
            serviceBusConnection.TopicName = requestParams.TargetTopic;

            if ((!string.IsNullOrEmpty(filePath)) && (!string.IsNullOrEmpty(filePath)))
            {
                data.fileName = filePath;
                data.fileContent = Encoding.Default.GetBytes(fileData);
                data.sourcePath = filePath;
                data.localCopy = false;
                data.BlobPath = fileBlobPath;
            }

            var satgeFunctionParams = new StageFunctionParams()
            {
                BatchOrder = 1,
                TransactionId = transactionId,
                Function = functionParams,
                Data = data,
                FileName = filePath,
                StartDate = startDate,
                BatchId = CommonUtilities.GetBatchId(),
                ServiceBusSettings = serviceBusConnection,
                File = data,
                FunctionName = RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT,
                TelemetryClient = telemetryClient,
                CustomMessageProperties = FunctionUtilities.GetCustomMessageProperties(functionParams.Settings.GetValue("Message Properties", ""))
            };

            var messageSender = new MessageSender(tenant.ServicebusConnectionString, requestParams.TargetTopic);

            FunctionUtilities.ProcessData(satgeFunctionParams, tenant, null, messageSender);

            await messageSender.CloseAsync();

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, $"TransactionId: {transactionId}, Published {data.fileName} to topic.");

            return transactionId;
        }

        private async static Task<(bool, string, string)> GetServiceDataAsync(ProductExportRequestParams requestParams, TelemetryClient telemetryClient, string serviceUri, string requestBody, string token)
        {
            var timeout = Convert.ToInt32(Environment.GetEnvironmentVariable("HttpRequestTimeout"));

            using (var httpClient = new HttpClient())
            {
                httpClient.Timeout = TimeSpan.FromSeconds(timeout);

                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                httpClient.DefaultRequestHeaders.Add("OUN", requestParams.Security.Settings.GetValue("OUN", string.Empty));

                using (var request = new HttpRequestMessage(HttpMethod.Post, serviceUri))
                {
                    request.Content = new StringContent(requestBody, Encoding.UTF8, "application/json");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, $"Calling api {serviceUri}...");

                    var httpResponse = await httpClient.SendAsync(request);

                    var serviceResponse = await httpResponse.Content.ReadAsStringAsync();

                    if (httpResponse.StatusCode == HttpStatusCode.OK)
                    {
                        var serviceResponseObject = JsonConvert.DeserializeObject<D365RetailServiceResponse>(serviceResponse);

                        if (!serviceResponseObject.Success)
                        {
                            return (false, string.Join(Environment.NewLine, serviceResponseObject.Errors), string.Empty);
                        }

                        return (true, string.Empty, serviceResponse);
                    }
                    else
                    {
                        telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT, $"Api error response: {serviceResponse}");

                        var serviceResponseObject = JsonConvert.DeserializeObject<D365GenericErrorResponse>(serviceResponse);

                        var exception = JsonConvert.DeserializeObject<D365GenericException>(serviceResponseObject.Exception);

                        return (false, exception.LocalizedMessage, string.Empty);
                    }
                }
            }
        }

        private async static Task<string> GetClientCredentialsAccessTokenAsync(HttpSecurity security)
        {
            var adTenant = security.Settings.GetValue("ADTenant", string.Empty);
            var adClientId = security.Settings.GetValue("ADClientAppId", string.Empty);
            var adResource = security.Settings.GetValue("ADResource", string.Empty);
            var adClientSecret = security.Settings.GetValue("ADClientAppSecret", string.Empty);

            var authenticationContext = new AuthenticationContext(adTenant, false);

            var credentials = new ClientCredential(adClientId, adClientSecret);
            var authenticationResult = await authenticationContext.AcquireTokenAsync(adResource, credentials);

            return authenticationResult.AccessToken;
        }

        private static ProductExportRequestParams GetProductExportRequestParams(FunctionParams functionParams, TenantConfigModel tenantConfig, TelemetryClient telemetryClient, int cacheExpireInMinutes)
        {
            var functionSettings = functionParams.Settings;

            var d365ConfigEntityName = functionSettings.GetValue("D365ConfigEntityName");
            var d365Config = CommonUtility.GetD365ServiceConfigurations($"{tenantConfig.Id}_D365Configurations_HttpReceive", tenantConfig.CloudDb, telemetryClient, cacheExpireInMinutes, d365ConfigEntityName,  RCKFunctionNames.FUNC_RCK_D365_PRODUCT_EXPORT);

            return new ProductExportRequestParams
            {
                TargetTopic = functionSettings.GetValue("Service Bus Target Topic", ""),
                ProductDetailsAPIRequestBody = JsonConvert.DeserializeObject<GetProductDetailsRequest>(functionSettings.GetValue("ProductDetailsRequestBody")),
                NoOfThreads = FunctionUtilities.GetIntValue(functionSettings.GetValue("NoOfThreads", ""), 5),
                GetChangedProductApiUri = functionSettings.GetValue("GetChangedProductApiUri", ""),
                GetProductDetailsApiUri = functionSettings.GetValue("GetProductDetailsApiUri", ""),
                BatchSize = FunctionUtilities.GetIntValue(functionSettings.GetValue("BatchSize"), 1000),
                Security = JsonConvert.DeserializeObject<HttpSecurity>(d365Config.D365BaseConfiguration.SecuritySettings)
            };
        }
    }
}