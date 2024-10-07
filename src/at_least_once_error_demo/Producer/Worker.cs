//Copyright (c) Microsoft Corporation. All rights reserved.
//Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//Licensed under the MIT License.
//Licensed under the Apache License, Version 2.0
//
//Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

using Confluent.Kafka;

namespace ReactiveEvents;

public class Worker
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

            var eventType = "TypeA";  // Randomly choose between two event types
            var y = rnd.Next(0, 10000);
            var msg = $"Sample {eventType} message #{y} sent at {DateTime.Now:yyyy-MM-dd_HH:mm:ss.ffff}";
            Console.WriteLine("Sending message: " + msg);
            await producer.ProduceAsync(topic, new Message<int, string> { Key = y, Value = msg });
            
        }
    }

    public static void Consumer(string? brokerList, string? connStr, string? consumergroup, string? topic, string? cacertlocation)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            SecurityProtocol = SecurityProtocol.Plaintext,
            SocketTimeoutMs = 60000,                //this corresponds to the Consumer config `request.timeout.ms`
            SessionTimeoutMs = 30000,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "$ConnectionString",
            SaslPassword = connStr,
            SslCaLocation = cacertlocation,
            GroupId = consumergroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            BrokerVersionFallback = "1.0.0",        //Event Hubs for Kafka Ecosystems supports Kafka v1.0+, a fallback to an older API will fail
            //Debug = "security,broker,protocol"    //Uncomment for librdkafka debugging information
        };

        using (var consumer = new ConsumerBuilder<int, string>(config).SetKeyDeserializer(Deserializers.Int32).SetValueDeserializer(Deserializers.Utf8).Build())
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

            consumer.Subscribe(topic);

            Console.WriteLine("Consuming messages from topic: " + topic + ", broker(s): " + brokerList);

            Random rnd = new Random();
                
            while (true)
            {
                try
                {
                    var msg = consumer.Consume(cts.Token);
                    Console.WriteLine($"Received: '{msg.Message.Value}'");

                    if(rnd.Next(0, 2) == 0)
                    {
                        Console.WriteLine("Time.. to die...");
                        throw new Exception("Something went wrong");
                    }

                    Console.WriteLine($"Completed processing: '{msg.Message.Value}'");

                    consumer.Commit(msg);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
    }
}
