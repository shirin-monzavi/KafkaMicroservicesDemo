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
            Quantity = 2
        };

        var message = JsonSerializer.Serialize(orderCreated);
        var messageId = orderCreated.MessageId.ToString();

        await _producer.PublishAsync(
            "order-created",
            messageId,
            message
         );

        return Ok(new
        {
            MessageId = messageId
        });
    }
}