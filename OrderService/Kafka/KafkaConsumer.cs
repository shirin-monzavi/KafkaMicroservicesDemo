using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using System.Text.Json;

namespace OrderService.Kafka;

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
            GroupId = "order-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();

        _consumer.Subscribe("stock-reservation-failed");
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                var failedEvent =
     JsonSerializer.Deserialize<StockReservationFailedEvent>(
         result.Message.Value);

                if (failedEvent is null)
                {
                    continue;
                }

                using var scope = _scopeFactory.CreateScope();

                var db = scope.ServiceProvider
                    .GetRequiredService<OrderDbContext>();

                var order = await db.Orders
                    .FirstOrDefaultAsync(x => x.Id == failedEvent.OrderId);

                if (order is null)
                {
                    Console.WriteLine(
                        $"Order not found: {failedEvent.OrderId}");

                    continue;
                }

                order.Status = OrderStatus.Cancelled;

                await db.SaveChangesAsync();

                Console.WriteLine(
                    $"Order {order.Id} cancelled. Reason: {failedEvent.Reason}");

                _consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _consumer.Close();

        await Task.CompletedTask;
    }
}