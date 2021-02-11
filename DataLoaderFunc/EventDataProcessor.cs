using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace DataLoaderFunc
{
    public static class EventDataProcessor
    {
        /// <summary>
        /// Reads Event Hub messages and writes those to a queue
        /// </summary>
        /// <param name="events"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("EventDataProcessor")]
        public static async Task RunEventDataProcessor([EventHubTrigger("attdataloadereh", Connection = "ehconn")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    string qname = Environment.GetEnvironmentVariable("qname");
                    await CreateQueueAndSendMessage(qname, messageBody, log);
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static async Task CreateQueueAndSendMessage(string queueName, string messageBody, ILogger log)
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
    }
}
