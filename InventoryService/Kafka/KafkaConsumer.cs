using Confluent.Kafka;
using InventoryService.Data;
using InventoryService.Events;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

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

                var orderCreatedEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

                if (orderCreatedEvent is null)
                {
                    throw new Exception("Invalid ordercreated event");
                }

                using var scope = _scopeFactory.CreateScope();

                var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

                var messageId = result.Message.Key;

                var alreadyProcessed = await db.ProcessedMessages.AnyAsync(x => x.MessageId == messageId, stoppingToken);

                if (alreadyProcessed)
                {
                    Console.WriteLine(
                        $"Duplicate message ignored: {messageId}");

                    _consumer.Commit(result);

                    continue;
                }

                await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);

                var rowsAffected = await db.Products.Where(x => x.Id == orderCreatedEvent.ProductId &&
                                                          x.Stock >= orderCreatedEvent.Quantity)
                                              .ExecuteUpdateAsync(s => s.SetProperty(x => x.Stock, x => x.Stock - orderCreatedEvent.Quantity),
                                              cancellationToken: stoppingToken);
                if (rowsAffected == 0)
                {
                    throw new Exception(
                        "Product not found or not enough stock.");
                }

                db.ProcessedMessages.Add(
                    new ProcessedMessage
                    {
                        MessageId = messageId,
                        ProcessedAt = DateTime.UtcNow
                    });

                await db.SaveChangesAsync(stoppingToken);

                await transaction.CommitAsync(stoppingToken);

                throw new Exception("Crash before Kafka commit");

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