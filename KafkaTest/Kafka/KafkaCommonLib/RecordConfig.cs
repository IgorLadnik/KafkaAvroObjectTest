using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using HttpClientLib;
using Avro;

namespace KafkaCommonLib
{
    public class RecordConfig
    {
        public string SchemaString { get; }
        public string Subject { get; }
        public int Version { get; }
        public int Id { get; }
        public RecordSchema RecordSchema { get; }

        public RecordConfig(string schemaRegistryUrl)
        {
            var dctProp = GetSchemaString(schemaRegistryUrl);

            SchemaString = dctProp.TryGetValue("schema", out object schemaOb) ? schemaOb.ToString() : null;
            Subject = dctProp.TryGetValue("subject", out object subjectOb) ? subjectOb.ToString() : null;
            Version = dctProp.TryGetValue("version", out object versionOb) ? (int)((dynamic)versionOb).Value : -1;
            Id = dctProp.TryGetValue("id", out object idOb) ? (int)((dynamic)idOb).Value : -1;

            RecordSchema = (RecordSchema)RecordSchema.Parse(SchemaString);
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
