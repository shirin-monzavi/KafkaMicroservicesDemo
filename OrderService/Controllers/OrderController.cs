using Microsoft.AspNetCore.Mvc;
using OrderService.Kafka;

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
        var orderId = Guid.NewGuid().ToString();

        await _producer.PublishAsync(
            "order-created",
            orderId,
            $"Order {orderId} created");

        return Ok(new
        {
            OrderId = orderId
        });
    }
}