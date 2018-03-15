using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace RdKafkaDotNetConsumer
{
    class Program
    {
        public static void Main(string[] args)
        {
            //string brokerList = "40.118.249.252";
            string brokerList = "138.91.140.244";
            //string brokerList = "localhost";
            List<String> topicList = new List<string>();
            topicList.Add("testLikeWin");
            var topics = topicList;

            var config = new Config() { GroupId = "simple-csharp-consumer" };
            using (var consumer = new EventConsumer(config, brokerList))
            {
                consumer.OnMessage += (obj, msg) =>
                {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
                };

                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topics.First(), 0, 5) });
                consumer.Start();

                Console.WriteLine("Started consumer, press enter to stop consuming");
                Console.ReadLine();
            }
        }
    }
}
