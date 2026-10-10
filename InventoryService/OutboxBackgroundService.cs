using InventoryService.Data;
using InventoryService.Kafka;
using Microsoft.EntityFrameworkCore;

namespace InventoryService;

public class OutboxBackgroundService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaProducer _producer;
    private readonly ILogger<OutboxBackgroundService> _logger;

    public OutboxBackgroundService(
        IServiceScopeFactory scopeFactory,
        KafkaProducer producer,
        ILogger<OutboxBackgroundService> logger)
    {
        _scopeFactory = scopeFactory;
        _producer = producer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();

                var db = scope.ServiceProvider
                    .GetRequiredService<InventoryDbContext>();

                var messages = await db.OutboxMessages
                    .Where(x => x.PublishedAt == null)
                    .OrderBy(x => x.Id)
                    .Take(20)
                    .ToListAsync(stoppingToken);


                foreach (var message in messages)
                {
                    await _producer.PublishAsync(
                        message.Topic,
                        message.Key,
                        message.Payload);

                    message.PublishedAt = DateTime.UtcNow;

                    await db.SaveChangesAsync(stoppingToken);

                    _logger.LogInformation(
                        "Outbox message {MessageId} published to {Topic}",
                        message.MessageId,
                        message.Topic);
                }
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error while processing outbox messages");
            }

            await Task.Delay(
                TimeSpan.FromSeconds(2),
                stoppingToken);
        }
    }

}