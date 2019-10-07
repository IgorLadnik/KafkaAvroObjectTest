using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Avro.Generic;
using HttpClientLib;
using KafkaProducerLib;
using KafkaConsumerLib;
using Newtonsoft.Json.Linq;
using KafkaCommonLib;

namespace KafkaTest
{
    class Program
    {         
        static void Main(string[] args)
        {
            #region Config

            //var schemaRegistryUrl = "http://localhost:9797/schema.json";
            var schemaRegistryUrl = @"..\..\..\wwwroot\schema.json";

            var bootstrapServers = "localhost:9092";
            var topic = "quick-start";      // "stream-topic";
            var groupId = "consumer-group"; // "simple-consumer";

            int partition = 0;
            int offset = 0;

            var recordConfig = new RecordConfig(schemaRegistryUrl);

            #endregion // Config

            #region Kafka Consumer

            var kafkaConsumer = new KafkaConsumer(
                                             bootstrapServers,
                                             recordConfig,
                                             topic,
                                             groupId,
                                             partition,
                                             offset,
                                             (key, value, dt) =>
                                             {
                                                 Console.WriteLine($"Consumed Object:\nkey = {key}");
                                                 foreach (var field in value.Schema.Fields)
                                                     Console.WriteLine($"  {field.Name} = {value[field.Name]}");
                                             },
                                             e => Console.WriteLine(e))
                    .StartConsuming();

            #endregion // Kafka Consumer

            #region Create Kafka Producer 

            var kafkaProducer = new KafkaProducer(
                                            bootstrapServers,
                                            recordConfig,
                                            topic,
                                            //partition,
                                            //offset,
                                            e => Console.WriteLine(e));
 
            #endregion // Create Kafka Producer 

            var count = 0;
            var timer = new Timer(_ => 
            {
                var lstTuple = new List<Tuple<string, GenericRecord>>();
                for (var i = 0; i < 10; i++)
                {
                    count++;

                    #region Create GenericRecord Object

                    var gr = new GenericRecord(recordConfig.RecordSchema);
                    gr.Add("SEQUENCE", count);
                    gr.Add("ID", count);
                    gr.Add("CategoryID", count);
                    gr.Add("YouTubeCategoryTypeID", count);
                    gr.Add("CreationTime", DateTime.Now.Ticks);

                    lstTuple.Add(new Tuple<string, GenericRecord>($"{count}", gr));

                    #endregion // Create GenericRecord Object
                }

                kafkaProducer.Send(lstTuple.ToArray());
            }, 
            null, 0, 5000);
            
            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();

            timer.Dispose();
            kafkaProducer.Dispose();
            kafkaConsumer.Dispose();
        }

        private static IDictionary<string, object> GetSchemaString(string schemaRegistryUrl)
        {
            try
            {
                var str = Encoding.Default.GetString(new HttpClient().Get(schemaRegistryUrl, 100))
                                       .Replace(" ", string.Empty).Replace("\n", "").Replace("\r", "").Replace("\t", "").Replace("\\", "");

                var jOb = JObject.Parse(str);
                var dctProp = new Dictionary<string, object>();
                foreach (var property in jOb.Properties())
                    dctProp[property.Name] = property.Value;

                return dctProp;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return null;
            }
        }
    }
}

