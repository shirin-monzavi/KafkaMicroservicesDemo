using Microsoft.EntityFrameworkCore;
using System.Reflection.Emit;

namespace OrderService.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Status)
                .HasConversion<int>();
        });

        modelBuilder.Entity<ProcessedMessage>()
    .HasIndex(x => x.MessageId)
    .IsUnique();
    }
}