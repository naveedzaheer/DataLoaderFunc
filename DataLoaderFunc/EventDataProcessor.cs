using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace DataLoaderFunc
{
    public class EventDataProcessor
    {
        private readonly IConfigCacheClient configCacheClient;

        public EventDataProcessor(IConfigCacheClient configCacheClient)
        {
            this.configCacheClient = configCacheClient;
        }

        /// <summary>
        /// Reads Event Hub messages and writes those to a queue
        /// </summary>
        /// <param name="events"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        //[FunctionName("EventDataProcessor")]
        //public static async Task RunEventDataProcessor([EventHubTrigger("attdataloadereh", Connection = "ehconn")] EventData[] events, ILogger log)
        //{
        //    var exceptions = new List<Exception>();

        //    foreach (EventData eventData in events)
        //    {
        //        try
        //        {
        //            string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        //            string qname = Environment.GetEnvironmentVariable("qname");
        //            await CreateQueueAndSendMessage(qname, messageBody, log);
        //            await AddLogRecord(messageBody, log);
        //        }
        //        catch (Exception e)
        //        {
        //            // We need to keep processing the rest of the batch - capture this exception and continue.
        //            // Also, consider capturing details of the message that failed processing so it can be processed again later.
        //            exceptions.Add(e);
        //        }
        //    }

        //    // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

        //    if (exceptions.Count > 1)
        //        throw new AggregateException(exceptions);

        //    if (exceptions.Count == 1)
        //        throw exceptions.Single();
        //}

        
        [FunctionName("QueueDataProcessor")]
        public async Task RunQueueDataProcessor([QueueTrigger("eventbus", Connection = "storageQueueConnection")] CloudQueueMessage queueMessage, 
            ExecutionContext executionContext, ILogger log)
        {
            try
            {
                log.LogInformation($"C# Queue trigger function message received: {queueMessage.Id} - HostId:{executionContext.InvocationId}");
                string messageBody = queueMessage.AsString;
                string qname = Environment.GetEnvironmentVariable("qname");
                await CreateQueueAndSendMessage(qname, messageBody, log);
                string cacheVal = this.configCacheClient.GetItem("message");
                log.LogInformation($"C# Queue trigger function message processed: {queueMessage.Id} - HostId:{executionContext.InvocationId} CacheId:{cacheVal}");
            }
            catch (Exception ex)
            {
                log.LogInformation($"An error occurred processing the message: {queueMessage.Id} Error:{ex.Message} - HostId:{executionContext.InvocationId}");
            }
        }


        private async Task CreateQueueAndSendMessage(string queueName, string messageBody, ILogger log)
        {
            try
            {
                var connectionString = Environment.GetEnvironmentVariable("qstorageconn");
                QueueClient queueClient = new QueueClient(connectionString, queueName);
                log.LogInformation($"Successfully wrote to queue={queueName} message=[{messageBody}]");

                // Create the queue
                await queueClient.CreateIfNotExistsAsync();
                await queueClient.SendMessageAsync(messageBody);
            }
            catch (Exception ex)
            {
                log.LogError($"An error occurred while writing to queue={queueName} Error={ex.Message}");
            }
            return;
        }

        private async Task AddLogRecord(string messageBody, ILogger iLog)
        {
            var jArray = JArray.Parse(messageBody);
            string subject = jArray[0]["subject"].Value<string>();
            string fileName = subject.Substring(subject.LastIndexOf("/") + 1);

            var connectionString = Environment.GetEnvironmentVariable("qstorageconn");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration()); var cloudTableClient = storageAccount.CreateCloudTableClient();
            var loggingTable = cloudTableClient.GetTableReference("datarecords");
            await loggingTable.CreateIfNotExistsAsync();
            var log = new LogEntity(fileName);
            var insertOp = TableOperation.Insert(log);
            await loggingTable.ExecuteAsync(insertOp);
        }
    }

    public class LogEntity : TableEntity
    {
        public LogEntity(string fileName)
        {
            this.PartitionKey = fileName;
            this.RowKey = Guid.NewGuid().ToString("N");
        }
    }

    //public class LogEntity : Microsoft.WindowsAzure.Storage.Table.ITableEntity
    //{
    //    public LogEntity(string fileName)
    //    {
    //        this.PartitionKey = fileName;
    //        this.RowKey = Guid.NewGuid().ToString("N");
    //        this.Timestamp = DateTimeOffset.UtcNow;
    //        this.ETag = this.RowKey;
    //    }

    //    public string PartitionKey { get; set; }
    //    public string RowKey { get; set ; }
    //    public DateTimeOffset Timestamp { get; set; }
    //    public string ETag { get; set; }

    //    public void ReadEntity(IDictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> properties, Microsoft.WindowsAzure.Storage.OperationContext operationContext)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public IDictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> WriteEntity(Microsoft.WindowsAzure.Storage.OperationContext operationContext)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}
