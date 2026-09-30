using Microsoft.AspNetCore.Mvc;
using OrderService.Events;
using OrderService.Kafka;
using System.Text.Json;

namespace OrderService.Controllers;

[ApiController]
[Route("orders")]
public class OrdersController : ControllerBase
{
    private readonly KafkaProducer _producer;

    public OrdersController(KafkaProducer producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> Create()
    {
        var orderId = Guid.NewGuid();

        var orderCreated = new OrderCreatedEvent()
        {
            MessageId = Guid.NewGuid(),
            OrderId = orderId,
            ProductId = 1,
            Quantity = 8
        };

        var message = JsonSerializer.Serialize(orderCreated);
        var messageId = orderCreated.MessageId.ToString();

        await _producer.PublishAsync(
            "order-created",
            orderId.ToString(),
            message
         );

        return Ok(new
        {
            OrderId = orderId,
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