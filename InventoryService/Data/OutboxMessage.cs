namespace InventoryService.Data;

public class OutboxMessage
{
    public long Id { get; set; }

    public string MessageId { get; set; } = null!;

    public string EventType { get; set; } = null!;

    public string Topic { get; set; } = null!;

    public string Key { get; set; } = null!;

    public string Payload { get; set; } = null!;

    public DateTime CreatedAt { get; set; }

    public DateTime? PublishedAt { get; set; }
}
