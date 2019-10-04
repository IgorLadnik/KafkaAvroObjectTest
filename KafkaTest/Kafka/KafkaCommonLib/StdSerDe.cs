using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaCommonLib
{
    public class SerDeHelper
    {
        private AvroSerializer<GenericRecord> _avroSerializer;
        private AvroDeserializer<GenericRecord> _avroDeserializer;

        public SerDeHelper(RecordConfig recordConfig)
        {
            var schemaRegistry = new SchemaRegistryClient(new Schema(recordConfig.Subject, recordConfig.Version, recordConfig.Id, recordConfig.SchemaString)); //1
            _avroSerializer = new AvroSerializer<GenericRecord>(schemaRegistry);
            _avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry);
        }

        public async Task<byte[]> SerializeAsync(GenericRecord genericRecord, string topic) =>
           await _avroSerializer.SerializeAsync(genericRecord, new SerializationContext(MessageComponentType.Value, topic));

        public async Task<GenericRecord> DeserializeAsync(byte[] bts, bool isNull, string topic) =>
           await _avroDeserializer.DeserializeAsync(bts, isNull, new SerializationContext(MessageComponentType.Value, topic));
    }

    public static class SerDeTest
    {
        public static async Task<bool> Test(RecordConfig recordConfig) 
        {
            var z = 77;

            var gr0 = new GenericRecord(recordConfig.RecordSchema);
            gr0.Add("SEQUENCE", z);
            gr0.Add("ID", z);
            gr0.Add("CategoryID", z);
            gr0.Add("YouTubeCategoryTypeID", z);
            gr0.Add("CreationTime", (long)1395);

            var topic = "topic";

            var h = new SerDeHelper(recordConfig);
            var bts = await h.SerializeAsync(gr0, topic);

            var gr1 = await h.DeserializeAsync(bts, false, topic);

            return gr0["CreationTime"] == gr1["CreationTime"];
        }
    }

    //public static class StdSerDe
    //{
    //    // Convert an object to a byte array
    //    public static byte[] ToByteArray(this object t)
    //    {
    //        if (t == null)
    //            return null;

    //        using (var ms = new MemoryStream())
    //        {
    //            var bf = new BinaryFormatter();
    //            bf.Serialize(ms, t);
    //            return ms.ToArray();
    //        }
    //    }

    //    // Convert a byte array to an Object
    //    public static object ToObject(this byte[] bts)
    //    {
    //        using (var ms = new MemoryStream())
    //        {
    //            var binForm = new BinaryFormatter();
    //            ms.Write(bts, 0, bts.Length);
    //            ms.Seek(0, SeekOrigin.Begin);
    //            return binForm.Deserialize(ms);
    //        }
    //    }
    //}
}
