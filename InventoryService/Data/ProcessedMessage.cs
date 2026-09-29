namespace InventoryService.Data;

public class ProcessedMessage
{
    public int Id { get; set; }

    public string MessageId { get; set; } = null!;

    public DateTime ProcessedAt { get; set; }
}
