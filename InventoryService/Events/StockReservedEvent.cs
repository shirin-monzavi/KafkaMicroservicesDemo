namespace InventoryService.Events;

public class StockReservedEvent
{
    public Guid MessageId { get; set; }

    public Guid OrderId { get; set; }

    public int ProductId { get; set; }

    public int Quantity { get; set; }
}
