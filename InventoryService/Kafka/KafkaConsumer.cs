using Confluent.Kafka;
using InventoryService.Data;
using InventoryService.Events;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace InventoryService.Kafka;

public class KafkaConsumer : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaProducer _producer;
    public KafkaConsumer(IServiceScopeFactory scopeFactory, KafkaProducer producer)
    {
        _scopeFactory = scopeFactory;

        _producer = producer;

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

                var messageId = orderCreatedEvent.MessageId.ToString();

                var alreadyProcessed = await db.ProcessedMessages.AnyAsync(x => x.MessageId == messageId, stoppingToken);

                if (alreadyProcessed)
                {
                    Console.WriteLine(
                        $"Duplicate message ignored: {messageId}");

                    _consumer.Commit(result);

                    continue;
                }

                await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);

                Console.WriteLine(
                                    $"Processing MessageId={messageId}, " +
                                    $"ProductId={orderCreatedEvent.ProductId}, " +
                                    $"Quantity={orderCreatedEvent.Quantity}");

                var rowsAffected = await db.Products.Where(x => x.Id == orderCreatedEvent.ProductId &&
                                                          x.Stock >= orderCreatedEvent.Quantity)
                                              .ExecuteUpdateAsync(s => s.SetProperty(x => x.Stock, x => x.Stock - orderCreatedEvent.Quantity),
                                              cancellationToken: stoppingToken);

                Console.WriteLine(
                                    $"RowsAffected={rowsAffected}");

                db.ProcessedMessages.Add(new ProcessedMessage
                {
                    MessageId = messageId,
                    ProcessedAt = DateTime.UtcNow
                });

                if (rowsAffected == 0)
                {
                    Console.WriteLine(
                                     $"Insufficient stock. ProductId={orderCreatedEvent.ProductId}");

                    var failedEvent = new StockReservationFailedEvent
                    {
                        MessageId = Guid.NewGuid(),
                        OrderId = orderCreatedEvent.OrderId,
                        ProductId = orderCreatedEvent.ProductId,
                        Quantity = orderCreatedEvent.Quantity,
                        Reason = "InsufficientStock"
                    };

                    var reservationFailedOutboxMessage = new OutboxMessage
                    {
                        MessageId = failedEvent.MessageId.ToString(),
                        EventType = nameof(StockReservationFailedEvent),
                        Topic = "stock-reservation-failed",
                        Key = failedEvent.OrderId.ToString(),
                        Payload = JsonSerializer.Serialize(failedEvent),
                        CreatedAt = DateTime.UtcNow
                    };

                    db.OutboxMessages.Add(reservationFailedOutboxMessage);

                    await db.SaveChangesAsync(stoppingToken);

                    await transaction.CommitAsync(stoppingToken);

                    _consumer.Commit(result);

                    continue;
                }

                var stockReservedEvent = new StockReservedEvent
                {
                    MessageId = Guid.NewGuid(),
                    OrderId = orderCreatedEvent.OrderId,
                    ProductId = orderCreatedEvent.ProductId,
                    Quantity = orderCreatedEvent.Quantity
                };

                var stockReservedOutboxMessage = new OutboxMessage
                {
                    MessageId = stockReservedEvent.MessageId.ToString(),
                    EventType = nameof(StockReservedEvent),
                    Topic = "stock-reserved",
                    Key = orderCreatedEvent.OrderId.ToString(),
                    Payload = JsonSerializer.Serialize(stockReservedEvent),
                    CreatedAt = DateTime.UtcNow
                };

                db.OutboxMessages.Add(stockReservedOutboxMessage);

                await db.SaveChangesAsync(stoppingToken);

                await transaction.CommitAsync(stoppingToken);

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