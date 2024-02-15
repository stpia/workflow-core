using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;

namespace WorkflowCore.Providers.Azure.Services
{
    public class AzureStorageQueueProvider : IQueueProvider
    {
        private readonly ILogger _logger;
        
        private readonly Dictionary<QueueType, QueueClient> _queues = new Dictionary<QueueType, QueueClient>();

        public bool IsDequeueBlocking => false;

        public AzureStorageQueueProvider(string connectionString, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger<AzureStorageQueueProvider>();
            
            _queues[QueueType.Workflow] = new QueueClient(connectionString, "workflowcore-workflows");
            _queues[QueueType.Event] = new QueueClient(connectionString, "workflowcore-events");
            _queues[QueueType.Index] = new QueueClient(connectionString, "workflowcore-index");
        }

        public async Task QueueWork(string id, QueueType queue)
        {
            await _queues[queue].SendMessageAsync(id);
        }

        public async Task<string> DequeueWork(QueueType queue, CancellationToken cancellationToken)
        {
            var cloudQueue = _queues[queue];

            if (cloudQueue == null)
                return null;
            
            var msg = await cloudQueue.ReceiveMessageAsync();

            if (msg?.Value?.MessageId == null)
                return null;

            await cloudQueue.DeleteMessageAsync(msg.Value.MessageId, msg.Value.PopReceipt);
            return msg.Value.Body?.ToString();
        }

        public async Task Start()
        {
            foreach (var queue in _queues.Values)
            {
                await queue.CreateIfNotExistsAsync();
            }
        }

        public Task Stop() => Task.CompletedTask;

        public void Dispose()
        {
        }
    }
}
