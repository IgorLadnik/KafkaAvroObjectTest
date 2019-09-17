﻿#define GENERIC_RECORD

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

            var schemaRegistryUrl = "http://localhost:9797/schema.json";

            var bootstrapServers = "localhost:9092";
            var topic = "stream-topic";
            var groupId = "simple-consumer";

            int partition = 0;
            int offset = 0;

            var recordConfig = new RecordConfig(schemaRegistryUrl);

            #endregion // Config

            #region Kafka Consumer

#if GENERIC_RECORD
            var kafkaConsumer = new KafkaConsumer<GenericRecord>(
#else
            var kafkaConsumer = new KafkaConsumer<com.dv.cache_youtube_category_mapping>(                 
#endif

                                             bootstrapServers,
                                             recordConfig,
                                             topic,
                                             groupId,
                                             partition,
                                             offset,
                                             (key, value, dt) =>
                                             {
                                                 Console.WriteLine($"Consumed Object:\nkey = {key}");
#if GENERIC_RECORD
                                                 var genRecord = (GenericRecord)value;
                                                 foreach (var field in genRecord.Schema.Fields)
                                                     Console.WriteLine($"  {field.Name} = {genRecord[field.Name]}");
#else
                                                 var yt = (com.dv.cache_youtube_category_mapping)value;
                                                 Console.WriteLine($"  SEQUENCE = {yt.SEQUENCE}");
                                                 Console.WriteLine($"  ID = {yt.ID}");
                                                 Console.WriteLine($"  CategoryID = {yt.CategoryID}");
                                                 Console.WriteLine($"  YouTubeCategoryTypeID = {yt.YouTubeCategoryTypeID}");
                                                 Console.WriteLine($"  CreationTime = {yt.CreationTime}");
#endif
                                             },
                                             e => Console.WriteLine(e))
                    .StartConsuming();

#endregion // Kafka Consumer

#region Create Kafka Producer 

#if GENERIC_RECORD
            var kafkaProducer = new KafkaProducer<GenericRecord>(
#else
            var kafkaProducer = new KafkaProducer<com.dv.cache_youtube_category_mapping>(
#endif
                                               bootstrapServers,
                                               recordConfig,
                                               topic,
                                               partition,
                                               offset,
                                               e => Console.WriteLine(e));
 
#endregion // Create Kafka Producer 

            var count = 0;
            var timer = new Timer(_ => 
            {
#if GENERIC_RECORD
                var lstTuple = new List<Tuple<string, GenericRecord>>();
#else
                var lstTuple = new List<Tuple<string, com.dv.cache_youtube_category_mapping>>();
#endif
                for (var i = 0; i < 10; i++)
                {
                    count++;

#region Create GenericRecord Object

#if GENERIC_RECORD
                    var gr = new GenericRecord(recordConfig.RecordSchema);
                    gr.Add("SEQUENCE", count);
                    gr.Add("ID", count);
                    gr.Add("CategoryID", count);
                    gr.Add("YouTubeCategoryTypeID", count);
                    gr.Add("CreationTime", DateTime.Now.Ticks);

                    lstTuple.Add(new Tuple<string, GenericRecord>($"{count}", gr));
#else
                    var yt = new com.dv.cache_youtube_category_mapping
                    {
                        SEQUENCE = count,
                        ID = count,
                        CategoryID = count,
                        YouTubeCategoryTypeID = count,
                        CreationTime = DateTime.Now.Ticks
                    };

                    lstTuple.Add(new Tuple<string, com.dv.cache_youtube_category_mapping>($"{count}", yt));
#endif

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

//var schemaString =
//    "{" +
//            //"'subject':'cache-youtube-category-mapping-value'," +
//            //"'version':5," +
//            //"'id':322272," +
//            //"'schema':" +
//            //"{" +
//            "'type':'record'," +
//            "'name':'cache_youtube_category_mapping'," +
//            "'namespace':'com.dv'," +
//            "'fields':" +
//            "[" +
//                "{'name':'SEQUENCE',              'type':'int'}," +
//                "{'name':'ID',                    'type':'int'}," +
//                "{'name':'CategoryID',            'type':['null', 'int']}," +
//                "{'name':'YouTubeCategoryTypeID', 'type':['null', 'int']}," +
//                "{'name':'CreationTime',          'type':['null', 'long'], 'default':null}" +
//            "]" +
//    //"}" +
//    "}";
//schemaString = schemaString.Replace("\n", "").Replace("\r", "").Replace(" ", "").Replace("'", "\"");

//var subject = "cache-youtube-category-mapping-value";

//var schemaString = "{\"type\":\"record\",\"name\":\"cache_youtube_category_mapping\",\"namespace\":\"com.dv\",\"fields\":[{\"name\":\"SEQUENCE\",\"type\":\"int\"},{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"CategoryID\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"YouTubeCategoryTypeID\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"CreationTime\",\"type\":[\"null\",\"long\"],\"default\":null}]}";


//Here correct bootstrap servers:
//var bootstrapServers = "nycd-og-kafkacluster01.doubleverify.corp:9092,nycd-og-kafkacluster02.doubleverify.corp:9092,nycd-og-kafkacluster03.doubleverify.corp:9092";

//And here is correct schema registry:

//var schemaRegistryUrl = "http://d-og-schemaregistry.doubleverify.corp:8081";



//var schemaRegistryUrl = "http://d-og-schemaregistry.doubleverify.corp:8081";

//var schemaRegistryUrl = "http://d-og-schemaregistry.doubleverify.corp:8081/subjects/cache-youtube-category-mapping-value/versions/5";           
//var bootstrapServers = "nycs-og-kafkacluster01.doubleverify.prod:9092,nycs-og-kafkacluster01.doubleverify.prod:9092,nycs-og-kafkacluster01.doubleverify.prod:9092";
//var groupId =  "simple-C#-consumer"; //$"{Guid.NewGuid()}";
//var topic = "dv-cls-page-live";


