using System;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Azure;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using Azure.Identity;
using Azure.Storage.Sas;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Specialized;

namespace CoreAzure.Tre
{
       public class FileCopyFunction
    {
        [FunctionName("FileCopyFunction")]                    
        public static async Task Run(
            [ServiceBusTrigger("%QueueName%", Connection = "ServiceBusConnection")] 
            Message message,
            ILogger log)
        {
            // az login, az account set --name abce806a-805f-46ce-a94b-03e660b48e1c
            //var sourceAccountUri = new Uri(string.Concat("https://", blobUriBuilder.Host));
            //var blobServiceClient = new BlobServiceClient(sourceAccountUri, new DefaultAzureCredential());
            //var blobContainer = blobServiceClient.GetBlobContainerClient(blobUriBuilder.BlobContainerName);

            try
            {
                var payload = Encoding.UTF8.GetString(message.Body);
                log.LogInformation($"Received message: {payload}");

                var model = JsonConvert.DeserializeObject<MyMessageModel>(payload);

                // Source 
                var sourceBlobUriBuilder = new BlobUriBuilder(new Uri(model.Data.Url));
                var sourceContainerUri = new Uri(string.Concat("https://", sourceBlobUriBuilder.Host, "/", sourceBlobUriBuilder.BlobContainerName));
                var sourceBlobContainer = new BlobContainerClient(sourceContainerUri, new DefaultAzureCredential());
                var sourceBlob = sourceBlobContainer.GetBlobClient(sourceBlobUriBuilder.BlobName);
                var sourceBlobReadSas = await GetUserDelegationSasForBlob(sourceBlobContainer, sourceBlob, 
                    BlobSasPermissions.Read | BlobSasPermissions.List);
                log.LogInformation($"Generated user delegation SAS for Read: {sourceBlobReadSas}");

                // Destination
                var destinationAccountUri = Environment.GetEnvironmentVariable("DestinationStorageAccount");
                if(string.IsNullOrEmpty(destinationAccountUri))
                {
                    log.LogInformation("Failed to retrieve destination storage account. Has it been set set in the App Setting?");
                }
                var destBlobServiceClient = new BlobServiceClient(new Uri(destinationAccountUri), new DefaultAzureCredential());
                var destBlobContainer = destBlobServiceClient.GetBlobContainerClient(sourceBlobContainer.Name);
                if(destBlobContainer == null)
                {
                    destBlobContainer = await destBlobServiceClient.CreateBlobContainerAsync(sourceBlobContainer.Name);
                }
                var destBlob = destBlobContainer.GetBlobClient(sourceBlobUriBuilder.BlobName);
                var destBlobWriteSas = await GetUserDelegationSasForBlob(destBlobContainer, destBlob, 
                    BlobSasPermissions.Read | BlobSasPermissions.List | BlobSasPermissions.Write);
                log.LogInformation($"Generated user delegation SAS for Write: {sourceBlobReadSas}");
 
                //Copy from source to destination, using SAS
                await CopySasToSasUri(sourceBlobReadSas, destBlobWriteSas);
                log.LogInformation($"Copied Blob from {model.Data.Url} to destination.");

                await sourceBlob.DeleteIfExistsAsync();
                log.LogInformation($"Delete Blob from {model.Data.Url}.");
            }
            catch (Exception ex)
            {
                log.LogInformation($"Failed to execute successfully: {ex.ToString()}");
                
                // TODO error handling of Service Bus Message to retry queue?
            }
        }

        async static Task<Uri> GetUserDelegationSasForBlob(BlobContainerClient blobContainerClient, BlobClient blobClient, BlobSasPermissions permissions)
        {
            var blobServiceClient = blobContainerClient.GetParentBlobServiceClient();

            // Get a user delegation key for the Blob service that's valid for 10 minutes.
            Azure.Storage.Blobs.Models.UserDelegationKey userDelegationKey =
                await blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow,
                                                                DateTimeOffset.UtcNow.AddMinutes(10));

            // Create a SAS token that's also valid for 10 minutes.
            var sasBuilder = new BlobSasBuilder()
            {
                BlobName = blobClient.Name,
                BlobContainerName = blobContainerClient.Name,
                Resource = "b", //Blob
                StartsOn = DateTimeOffset.UtcNow,
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
            };

            sasBuilder.SetPermissions(permissions);

            // Add the SAS token to the container URI.
            var blobUriBuilder = new BlobUriBuilder(blobClient.Uri)
            {
                // Specify the user delegation key.
                Sas = sasBuilder.ToSasQueryParameters(userDelegationKey,
                                                    blobServiceClient.AccountName)
            };

            return blobUriBuilder.ToUri();
        }

        async static Task<Uri> GetUserDelegationSasForContainer(BlobContainerClient blobContainerClient, BlobContainerSasPermissions permissions)
        {
            BlobServiceClient blobServiceClient = blobContainerClient.GetParentBlobServiceClient();

            // Get a user delegation key for the Blob service that's valid for 10 minutes.
            Azure.Storage.Blobs.Models.UserDelegationKey userDelegationKey =
                await blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow,
                                                                DateTimeOffset.UtcNow.AddMinutes(10));

            // Create a SAS token that's also valid for 10 minutes.
            BlobSasBuilder sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = blobContainerClient.Name,
                Resource = "c", //Container
                StartsOn = DateTimeOffset.UtcNow,
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
            };

            sasBuilder.SetPermissions(permissions);

            // Add the SAS token to the container URI.
            BlobUriBuilder blobUriBuilder = new BlobUriBuilder(blobContainerClient.Uri)
            {
                // Specify the user delegation key.
                Sas = sasBuilder.ToSasQueryParameters(userDelegationKey,
                                                    blobServiceClient.AccountName)
            };

            return blobUriBuilder.ToUri();
        }

        async static Task CopySasToSasUri(Uri sourceUri, Uri targetUri)
        {
            var toBlob = new BlockBlobClient(targetUri);
            await toBlob.StartCopyFromUriAsync(sourceUri); 
            await WaitForPendingCopyToCompleteAsync(toBlob);
        }

        async static Task WaitForPendingCopyToCompleteAsync(BlockBlobClient toBlob)
        {
            var props = await toBlob.GetPropertiesAsync();
            while (props.Value.BlobCopyStatus == CopyStatus.Pending)
            {
                await Task.Delay(1000);
                props = await toBlob.GetPropertiesAsync();
            }

            if (props.Value.BlobCopyStatus != CopyStatus.Success)
            {
                throw new InvalidOperationException($"Copy failed: {props.Value.BlobCopyStatus}");
            }
        }
    }

     public class MyMessageModel
    {
        public string Topic { get; set; }
        public string Subject { get; set; }
        public string EventType { get; set; }
        public Guid Id { get; set; }
        public DataModel Data { get; set; }
        public DateTime EventTime { get; set; }

        public class DataModel
        {
            public string Url { get; set; }
        }
    }
}
