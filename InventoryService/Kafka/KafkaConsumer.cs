using Confluent.Kafka;
using InventoryService.Data;
using Microsoft.EntityFrameworkCore;

namespace InventoryService.Kafka;

public class KafkaConsumer : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IServiceScopeFactory _scopeFactory;

    public KafkaConsumer(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory;

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

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                using var scope = _scopeFactory.CreateScope();

                var db = scope.ServiceProvider
                    .GetRequiredService<InventoryDbContext>();

                var messageId = result.Message.Key;

                var alreadyProcessed =
                                     await db.ProcessedMessages
                                         .AnyAsync(x => x.MessageId == messageId, stoppingToken);

                if (alreadyProcessed)
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

                db.ProcessedMessages.Add(
                    new ProcessedMessage
                    {
                        MessageId = messageId,
                        ProcessedAt = DateTime.UtcNow
                    });

                await db.SaveChangesAsync(stoppingToken);

                _consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _consumer.Close();

    }
}