
using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Confluent.Kafka;

namespace ReactiveEvents;

public class ReactiveWorker
{
    public static async Task Producer(string? brokerList, string? connStr, string? topic, string? cacertlocation)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            SecurityProtocol = SecurityProtocol.Plaintext,
        };

        using (var producer = new ProducerBuilder<int, string>(config).SetKeySerializer(Serializers.Int32).SetValueSerializer(Serializers.Utf8).Build())
        {
            Random rnd = new Random();
            
            while (true)
            {
                var eventType = rnd.Next(0, 2) == 0 ? "TypeA" : "TypeB";  // Randomly choose between two event types
                var y = rnd.Next(0, 10000);
                var msg = $"Sample {eventType} message #{y} sent at {DateTime.Now:yyyy-MM-dd_HH:mm:ss.ffff}";
                await producer.ProduceAsync(topic, new Message<int, string> { Key = y, Value = msg });
            }
        }
    }

    public static void Consumer(string? brokerList, string? connStr, string? consumergroup, string? topic, string? cacertlocation)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            SecurityProtocol = SecurityProtocol.Plaintext,
            GroupId = consumergroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using (var consumer = new ConsumerBuilder<int, string>(config).SetKeyDeserializer(Deserializers.Int32).SetValueDeserializer(Deserializers.Utf8).Build())
        {
            // Create a subject to push consumed messages into a stream
            var subject = new Subject<(int Key, string Value)>();

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

            consumer.Subscribe(topic);

            Console.WriteLine("Consuming messages from topic: " + topic);

            // Start a task to continually consume messages
            Task.Run(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var msg = consumer.Consume(cts.Token);
                        Console.WriteLine($"Received: '{msg.Message.Value}'");
                        subject.OnNext((msg.Message.Key, msg.Message.Value));  // Push message to the subject
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            });

            // Set up reactive logic to wait until both TypeA and TypeB messages are received with the same key
            var typeAStream = subject.Where(msg => msg.Value.Contains("TypeA"));
            var typeBStream = subject.Where(msg => msg.Value.Contains("TypeB"));

            // Combine the streams and process when both event types are received with the same key
            typeAStream
                .GroupBy(msg => msg.Key)  // Group messages by Key
                .SelectMany(groupA =>
                    groupA.Zip(
                        typeBStream.Where(msg => msg.Key == groupA.Key),  // Match with TypeB messages with the same key
                        (a, b) => (a, b))  // Combine TypeA and TypeB messages
                )
                .Subscribe(pair =>
                {
                    var (typeAMsg, typeBMsg) = pair;
                    Console.WriteLine($"Matched events for key {typeAMsg.Key}: TypeA='{typeAMsg.Value}', TypeB='{typeBMsg.Value}'");
                    
                });

            // Keep the program running until cancelled
            cts.Token.WaitHandle.WaitOne();
        }
    }
}
