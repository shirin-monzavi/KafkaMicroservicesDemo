#nullable disable
namespace OrderService.Kafka;

public class StockReservationFailedEvent
{
    public Guid MessageId { get; set; }
    public Guid OrderId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public string Reason { get; set; }
}
