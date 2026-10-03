using Microsoft.EntityFrameworkCore;

namespace InventoryService.Data;

public class InventoryDbContext : DbContext
{
    public InventoryDbContext(
    DbContextOptions<InventoryDbContext> options)
    : base(options)
    {
    }

    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    public DbSet<Product> Products => Set<Product>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(
        ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedMessage>()
            .HasIndex(x => x.MessageId)
            .IsUnique();

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.HasIndex(x => x.MessageId)
                .IsUnique();

            entity.Property(x => x.EventType)
                .HasMaxLength(200);

            entity.Property(x => x.Topic)
                .HasMaxLength(200);

            entity.Property(x => x.Key)
                .HasMaxLength(450);
        });
    }
}
