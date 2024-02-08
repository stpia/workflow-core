#nullable enable

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using WorkflowCore.Interface;
using WorkflowCore.Models.LifeCycleEvents;

namespace WorkflowCore.Providers.Azure.Services
{
    public class ServiceBusLifeCycleEventHub : ILifeCycleEventHub
    {
        private readonly string _queueName;
        private readonly ServiceBusSender _sender;
        private ServiceBusProcessor? _processor;
        private readonly ILogger _logger;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ICollection<Action<LifeCycleEvent>> _subscribers = new HashSet<Action<LifeCycleEvent>>();
        private readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All,
            ReferenceLoopHandling = ReferenceLoopHandling.Error,
        };

        public ServiceBusLifeCycleEventHub(
            string connectionString,
            string topicName,
            string queueName,
            ILoggerFactory logFactory)
        {
            _serviceBusClient = new ServiceBusClient(connectionString);
            _sender = _serviceBusClient.CreateSender(topicName);
            _logger = logFactory.CreateLogger(GetType());
            _queueName = queueName;
        }

        public async Task PublishNotification(LifeCycleEvent evt)
        {
            var payload = JsonConvert.SerializeObject(evt, _serializerSettings);
            var message = new ServiceBusMessage(Encoding.Default.GetBytes(payload))
            {
                Subject = evt.Reference
            };

            await _sender.SendMessageAsync(message);
        }

        public void Subscribe(Action<LifeCycleEvent> action)
        {
            _subscribers.Add(action);
        }

        public Task Start()
        {
            var messageHandlerOptions = new ServiceBusProcessorOptions()
            {
                AutoCompleteMessages = false
            };

            _processor = _serviceBusClient.CreateProcessor(_queueName);
            _processor.ProcessMessageAsync += MessageHandler;
            _processor.ProcessErrorAsync += ExceptionHandler;

            return Task.CompletedTask;
        }

        public async Task Stop()
        {
            await _sender.CloseAsync();
            await (_processor?.StopProcessingAsync() ?? Task.CompletedTask);
        }

        private async Task MessageHandler(ProcessMessageEventArgs args)
        {
            try
            {
                var payload = Encoding.Default.GetString(args.Message.Body);
                var evt = JsonConvert.DeserializeObject<LifeCycleEvent>(
                    payload, _serializerSettings);

                if (evt == null)
                {
                    await args.AbandonMessageAsync(args.Message);
                    return;
                }

                NotifySubscribers(evt);

                await args.CompleteMessageAsync(args.Message)
                    .ConfigureAwait(false);
            }
            catch
            {
                await args.AbandonMessageAsync(args.Message);
            }
        }

        private Task ExceptionHandler(ProcessErrorEventArgs arg)
        {
            _logger.LogWarning(default, arg.Exception, "Error on receiving events");

            return Task.CompletedTask;
        }

        private void NotifySubscribers(LifeCycleEvent evt)
        {
            foreach (var subscriber in _subscribers)
            {
                try
                {
                    subscriber(evt);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        default, ex, $"Error on event subscriber: {ex.Message}");
                }
            }
        }
    }
}
