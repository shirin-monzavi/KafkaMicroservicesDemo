using Confluent.Kafka;

namespace OrderService.Kafka;

public class KafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        _producer = new ProducerBuilder<string, string>(config)
            .Build();
    }

    public async Task PublishAsync(
        string topic,
        string key,
        string message)
    {
        var result = await _producer.ProduceAsync(
            topic,
            new Message<string, string>
            {
                Key = key,
                Value = message
            });

        Console.WriteLine(
            $"Message delivered to Order: {result.Partition} {result.TopicPartitionOffset}");
    }
}