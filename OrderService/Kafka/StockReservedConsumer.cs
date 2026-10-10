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

                    var alreadyProcessed =
                        await db.ProcessedMessages.AnyAsync(
                            x => x.MessageId == messageId,
                            stoppingToken);

                    if (alreadyProcessed)
                    {
                        _logger.LogInformation(
                            "Duplicate StockReservedEvent ignored: {MessageId}",
                            messageId);

                        _consumer.Commit(result);
                        continue;
                    }

                    await using var transaction =
                        await db.Database.BeginTransactionAsync(
                            stoppingToken);

                    var order = await db.Orders.FirstOrDefaultAsync(
                        x => x.Id == stockReservedEvent.OrderId,
                        stoppingToken);

                    if (order is null)
                    {
                        _logger.LogWarning(
                            "Order {OrderId} was not found.",
                            stockReservedEvent.OrderId);

                        // Do not commit the offset here.
                        // We will handle this case in our retry/error design.
                        continue;
                    }

                    if (order.Status == OrderStatus.Created)
                    {
                        order.Status = OrderStatus.Confirmed;
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Order {OrderId} cannot transition from {Status} to Confirmed.",
                            order.Id,
                            order.Status);
                    }

                    db.ProcessedMessages.Add(new ProcessedMessage
                    {
                        MessageId = messageId,
                        ProcessedAt = DateTime.UtcNow
                    });

                    await db.SaveChangesAsync(stoppingToken);

                    await transaction.CommitAsync(stoppingToken);

                    _consumer.Commit(result);

                    _logger.LogInformation(
                        "Order {OrderId} processed StockReservedEvent.",
                        order.Id);
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