
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Events;

namespace OrderService;

public class StockReservedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<StockReservedConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;

    public StockReservedConsumer(
        IServiceScopeFactory scopeFactory,
        ILogger<StockReservedConsumer> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "order-stock-reserved-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _consumer.Subscribe("stock-reserved");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(stoppingToken);

                    var stockReservedEvent =
                        JsonSerializer.Deserialize<StockReservedEvent>(
                            result.Message.Value);

                    if (stockReservedEvent is null)
                    {
                        throw new InvalidOperationException(
                            "Invalid StockReservedEvent.");
                    }

                    using var scope = _scopeFactory.CreateScope();

                    var db = scope.ServiceProvider
                        .GetRequiredService<OrderDbContext>();

                    var messageId =
                        stockReservedEvent.MessageId.ToString();

                    await using var transaction =
                        await db.Database.BeginTransactionAsync(
                            stoppingToken);

                    // 1. Check idempotency inside the transaction.
                    var alreadyProcessed =
                        await db.ProcessedMessages.AnyAsync(
                            x => x.MessageId == messageId,
                            stoppingToken);

                    if (alreadyProcessed)
                    {
                        await transaction.CommitAsync(stoppingToken);

                        _logger.LogInformation(
                            "Duplicate StockReservedEvent ignored: {MessageId}",
                            messageId);

                        _consumer.Commit(result);
                        continue;
                    }

                    // 2. Check whether the order exists.
                    var orderExists = await db.Orders.AnyAsync(
                        x => x.Id == stockReservedEvent.OrderId,
                        stoppingToken);

                    if (!orderExists)
                    {
                        _logger.LogWarning(
                            "Order {OrderId} was not found.",
                            stockReservedEvent.OrderId);

                        // Do not save ProcessedMessage or commit the offset.
                        // Retry/DLQ handling will be designed separately.
                        continue;
                    }

                    // 3. Atomically transition Created -> Confirmed.
                    var rowsAffected = await db.Orders
                        .Where(x =>
                            x.Id == stockReservedEvent.OrderId &&
                            x.Status == OrderStatus.Created)
                        .ExecuteUpdateAsync(
                            setters => setters.SetProperty(
                                x => x.Status,
                                OrderStatus.Confirmed),
                            stoppingToken);

                    if (rowsAffected == 1)
                    {
                        _logger.LogInformation(
                            "Order {OrderId} transitioned to Confirmed.",
                            stockReservedEvent.OrderId);
                    }
                    else
                    {
                        var currentStatus = await db.Orders
                            .Where(x => x.Id == stockReservedEvent.OrderId)
                            .Select(x => (OrderStatus?)x.Status)
                            .FirstOrDefaultAsync(stoppingToken);

                        if (currentStatus is null)
                        {
                            _logger.LogWarning(
                                "Order {OrderId} no longer exists.",
                                stockReservedEvent.OrderId);

                            // Do not mark this event as processed.
                            continue;
                        }

                        _logger.LogWarning(
                            "Order {OrderId} cannot transition from {Status} to Confirmed.",
                            stockReservedEvent.OrderId,
                            currentStatus);
                    }

                    // 4. Record the event in the same transaction.
                    db.ProcessedMessages.Add(new ProcessedMessage
                    {
                        MessageId = messageId,
                        ProcessedAt = DateTime.UtcNow
                    });

                    await db.SaveChangesAsync(stoppingToken);

                    // 5. Commit database changes before Kafka offset.
                    await transaction.CommitAsync(stoppingToken);

                    _consumer.Commit(result);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(
                        ex,
                        "Kafka error while consuming stock-reserved.");
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
}
