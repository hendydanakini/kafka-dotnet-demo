using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Net;

namespace KafkaDemo.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class CustomerJobController : ControllerBase
    {
        [HttpGet("Insert", Name = "Insert")]
        public async Task<IActionResult> Insert()
        {
            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            try
            {
                var producer = new ProducerBuilder<Null, string>(config).Build();
                for (int i = 0; i < 10000; i++)
                {
                    try
                    {
                        string valMod = null;
                        if (i == 15)
                        {
                            //valMod = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"double\",\"optional\":true,\"field\":\"balance\"}],\"optional\":false,\"name\":\"customer_info\"},\"payload\":{\"names\":\"Test Kafka Name " + i + "\",\"age\":\"18\",\"balance\":\"5550000\"}}";
                        }
                        var result = await producer.ProduceAsync
                        ("customer-sink-topic", new Message<Null, string>
                        {
                            Value = valMod ?? "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"double\",\"optional\":true,\"field\":\"balance\"}],\"optional\":false,\"name\":\"customer_info\"},\"payload\":{\"name\":\"Test Kafka Name " + i + "\",\"age\":18,\"balance\":5550000.0}}"
                        });
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error occured {i}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return new JsonResult(new
            {
                success = true
            });
        }



        [HttpGet("InsertDb", Name = "InsertDb")]
        public async Task<IActionResult> InsertDb()
        {
            // Connection string to connect to PostgreSQL
            var connectionString = "Host=localhost;Username=postgres;Password=admin;Database=kafka_test_hendy";

            // Create a new connection
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();

            // Define the insert command
            var insertCommandText = "INSERT INTO customer_info (name, age, balance) VALUES (@name, @age, @balance)";

            try
            {
                for (int i = 0; i < 10000; i++)
                {
                    await using var cmd = new NpgsqlCommand(insertCommandText, conn);

                    // Add parameters to avoid SQL injection
                    cmd.Parameters.AddWithValue("name", $"Test Db Insert {i}");
                    cmd.Parameters.AddWithValue("age", 14);
                    cmd.Parameters.AddWithValue("balance", 4250000.0);

                    // Execute the command
                    var rowsAffected = await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return new JsonResult(new
            {
                success = true
            });
        }

        [HttpGet("Select", Name = "Select")]
        public async Task<IActionResult> Select()
        {
            var config = new ConsumerConfig
            {
                GroupId = "dotnet-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("customer-sink-topic");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        // Poll for messages with a 100ms timeout
                        var consumeResult = consumer.Consume(cts.Token);

                        // Handle the consumed message
                        Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}' '{consumer.Name}'");
                    }
                    catch (ConsumeException ex)
                    {
                        // Handle consume error
                        Console.WriteLine($"Error occurred: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Graceful cancellation
                Console.WriteLine("Consumer canceled.");
            }
            finally
            {
                // Ensure the consumer leaves the group cleanly and commits final offsets
                consumer.Close();
            }

            return new JsonResult(new
            {
                success = true
            });
        }

        [HttpGet("SelectSource", Name = "SelectSource")]
        public async Task<IActionResult> SelectSource()
        {
            var config = new ConsumerConfig
            {
                GroupId = "dotnet-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("source-topic-customer_info");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        // Poll for messages with a 100ms timeout
                        var consumeResult = consumer.Consume(cts.Token);

                        // Handle the consumed message
                        Console.WriteLine($"Consumed source '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}' '{consumer.Name}'");
                    }
                    catch (ConsumeException ex)
                    {
                        // Handle consume error
                        Console.WriteLine($"Error occurred: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Graceful cancellation
                Console.WriteLine("Consumer canceled.");
            }
            finally
            {
                // Ensure the consumer leaves the group cleanly and commits final offsets
                consumer.Close();
            }

            return new JsonResult(new
            {
                success = true
            });
        }
    }
}
