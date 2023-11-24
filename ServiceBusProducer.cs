using Azure.Messaging.ServiceBus;
using System.Text.Json;

namespace ServiceBus.Core
{
	public abstract class ServiceBusProducer<T> : IAsyncDisposable
	{
		private readonly ServiceBusClient _client;
		private readonly ServiceBusSender _sender;

		public ServiceBusProducer(string connectionString, string queueName)
		{
			if (string.IsNullOrEmpty(connectionString))
				throw new ArgumentException($"'{nameof(connectionString)}' cannot be null or empty.", nameof(connectionString));

			var clientOptions = new ServiceBusClientOptions()
			{
				TransportType = ServiceBusTransportType.AmqpWebSockets
			};
			_client = new ServiceBusClient(connectionString, clientOptions);
			_sender = _client.CreateSender(queueName);
		}

		public async Task PostMessage(T item)
		{
			await PostMessages(new List<T>() { item });
		}

		public async Task PostMessages(IEnumerable<T> items)
		{
			// create a batch 
			ServiceBusMessageBatch messageBatch = await _sender.CreateMessageBatchAsync();
			try
			{
				foreach (var item in items)
				{
					var json = JsonSerializer.Serialize(item);

					// if it is too large for the batch, send the batch as it is.
					if (!messageBatch.TryAddMessage(new ServiceBusMessage(json)))
					{

						// Batch is full, send what we have.
						await _sender.SendMessagesAsync(messageBatch);
						Console.WriteLine($"A parital batch of {items.Count()} items has been published to the queue.");

						// Create a new batch and continue processing.
						messageBatch.Dispose();
						messageBatch = await _sender.CreateMessageBatchAsync();
					}
				}

				// All messages have been queued, send them.
				await _sender.SendMessagesAsync(messageBatch);
				Console.WriteLine($"The final batch of {items.Count()} items has been published to the queue.");
			}
			finally
			{
				messageBatch.Dispose();
			}
		}

		public async ValueTask DisposeAsync()
		{
			await _sender.DisposeAsync();
			await _client.DisposeAsync();
		}
	}
}
