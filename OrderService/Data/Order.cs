namespace OrderService.Data;

public class Order
{
    public Guid Id { get; set; }

    public int ProductId { get; set; }

    public int Quantity { get; set; }

    public OrderStatus Status { get; set; }

    public DateTime CreatedAt { get; set; }
}
