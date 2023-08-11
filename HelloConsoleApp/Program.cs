// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using RabbitMQ.Client;
using System.Text;
using RabbitMQ.Client.Events;

Console.WriteLine("Hello, World!");

const string exchange_name = "amq.direct";
const string routing_key = "direct_queue_key";
const string queue_name = "direct_queue";

var factory = new ConnectionFactory() { Uri = new Uri("amqp://guest:guest@localhost:5672") };
var connection = factory.CreateConnection();
var channel = connection.CreateModel();

//channel.ExchangeDeclare(exchange_name, ExchangeType.Direct, true);
//channel.QueueDeclare(queue_name, true, false, false, null);
//channel.QueueBind(queue_name, exchange_name, routing_key);

Task t = PublishMessageToExchange(channel);
ConsumeMessageFromExchange(channel);

Console.WriteLine($"Number of messages in Queue: {channel.MessageCount(queue_name)}");
Console.ReadLine();
channel.Close();


async Task PublishMessageToExchange(IModel channel)
{
    await Task.Run(() =>
    {
        for (int i = 0; i < 10; i++)
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { messageId = (i + 1), header = "hello", body = "world", time = DateTime.Now.ToString() }));
            channel.BasicPublish(exchange_name, routing_key, null, body);
            Console.WriteLine($"{DateTime.Now}:: Message:{i + 1} published");
            Thread.Sleep(1000);
        }
    });
}

void ConsumeMessageFromExchange(IModel channel)
{
    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (sender, args) =>
    {
        Console.WriteLine(DateTime.Now + "::Message consumed:\r\n" + Encoding.UTF8.GetString(args.Body.ToArray()));
    };

    channel.BasicConsume(queue_name, true, consumer);
}
