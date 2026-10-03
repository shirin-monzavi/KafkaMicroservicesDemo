using Microsoft.AspNetCore.Mvc;
using OrderService.Data;
using OrderService.Events;
using OrderService.Kafka;
using System.Text.Json;

namespace OrderService.Controllers;

[ApiController]
[Route("orders")]
public class OrdersController : ControllerBase
{
    private readonly KafkaProducer _producer;
    private readonly OrderDbContext _db;

    public OrdersController(
        KafkaProducer producer,
        OrderDbContext db
        )
    {
        _producer = producer;
        _db = db;
    }

    [HttpPost]
    public async Task<IActionResult> Create()
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            ProductId = 1,
            Quantity = 8,
            Status = OrderStatus.Created,
            CreatedAt = DateTime.UtcNow
        };

        _db.Orders.Add(order);

        await _db.SaveChangesAsync();

        var orderCreated = new OrderCreatedEvent()
        {
            MessageId = Guid.NewGuid(),
            OrderId = order.Id,
            ProductId = order.ProductId,
            Quantity = order.Quantity
        };

        var message = JsonSerializer.Serialize(orderCreated);
        var messageId = orderCreated.MessageId.ToString();

        await _producer.PublishAsync(
            "order-created",
            order.Id.ToString(),
            message
         );

        return Ok(new
        {
            OrderId = order.Id,
            MessageId = messageId
        });
    }

    [HttpPost("concurrent-test")]
    public async Task<IActionResult> ConcurrentTest()
    {
        var tasks = Enumerable.Range(1, 2)
            .Select(async _ =>
            {
                var orderId = Guid.NewGuid();

                var orderCreatedEvent = new OrderCreatedEvent
                {
                    MessageId = Guid.NewGuid(),
                    OrderId = orderId,
                    ProductId = 1,
                    Quantity = 8
                };

                var message = JsonSerializer.Serialize(orderCreatedEvent);

                await _producer.PublishAsync(
                    "order-created",
                    orderCreatedEvent.MessageId.ToString(),
                    message);
            });

        await Task.WhenAll(tasks);

        return Ok("Two orders sent.");
    }
}