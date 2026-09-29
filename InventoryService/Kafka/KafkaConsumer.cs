using Confluent.Kafka;

namespace InventoryService.Kafka;

public class KafkaConsumer : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly HashSet<string> _processedMessages = new();

    public KafkaConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "inventory-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .Build();

        _consumer.Subscribe("order-created");
    }

    protected override Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                var messageId = result.Message.Key;

                if (_processedMessages.Contains(messageId))
                {
                    Console.WriteLine(
                        $"Duplicate message ignored: {messageId}");

                    _consumer.Commit(result);

                    continue;
                }

                Console.WriteLine(
                    $"Processing order: {result.Message.Value}");

                // Business Logic
                Console.WriteLine("Order processed successfully.");

                _processedMessages.Add(messageId);

                _consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _consumer.Close();

        return Task.CompletedTask;
    }
}