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
        };

        var message = JsonSerializer.Serialize(orderCreated);

        await _producer.PublishAsync(
            "order-created",
            orderCreated.MessageId.ToString(),
            message
         );

        return Ok(new
        {
            OrderId = orderId
        });
    }
}