using System;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
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
        public static Task Run(
            [ServiceBusTrigger("%QueueName%", Connection = "ServiceBusConnection")] 
            Message message,
            ILogger log)
        {
            
            var payload = Encoding.UTF8.GetString(message.Body);
            log.LogInformation($"Function processed message: {payload}");

            var model = JsonConvert.DeserializeObject<MyMessageModel>(payload);

            return Task.CompletedTask;
        }

        async static Task<Uri> GetUserDelegationSasForContainer(BlobContainerClient blobContainerClient)
        {
            BlobServiceClient blobServiceClient = blobContainerClient.GetParentBlobServiceClient();

            // Get a user delegation key for the Blob service that's valid for seven days.
            // You can use the key to generate any number of shared access signatures 
            // over the lifetime of the key.
            Azure.Storage.Blobs.Models.UserDelegationKey userDelegationKey =
                await blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow,
                                                                DateTimeOffset.UtcNow.AddDays(7));

            // Create a SAS token that's also valid for seven days.
            BlobSasBuilder sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = blobContainerClient.Name,
                Resource = "c", //Container
                StartsOn = DateTimeOffset.UtcNow,
                ExpiresOn = DateTimeOffset.UtcNow.AddDays(7)
            };

            // Specify permissions for the SAS.
            sasBuilder.SetPermissions(
                BlobContainerSasPermissions.Read |
                BlobContainerSasPermissions.List
                );

            // Add the SAS token to the container URI.
            BlobUriBuilder blobUriBuilder = new BlobUriBuilder(blobContainerClient.Uri)
            {
                // Specify the user delegation key.
                Sas = sasBuilder.ToSasQueryParameters(userDelegationKey,
                                                    blobServiceClient.AccountName)
            };

            Console.WriteLine("Container user delegation SAS URI: {0}", blobUriBuilder);
            Console.WriteLine();
            return blobUriBuilder.ToUri();
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