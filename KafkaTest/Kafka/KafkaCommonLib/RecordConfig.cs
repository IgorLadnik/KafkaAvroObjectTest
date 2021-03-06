﻿using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Avro;
using HttpClientLib;

namespace KafkaCommonLib
{
    public class RecordConfig
    {
        public string SchemaString { get; }
        public string Subject { get; }
        public int Version { get; }
        public int Id { get; }
        public RecordSchema RecordSchema { get; private set; }

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
            string str;

            try 
            {
                str = Encoding.Default.GetString(new HttpClient().Get(schemaRegistryUrl, 100));
            } 
            catch
            {
                try
                {
                    str = System.IO.File.ReadAllText(schemaRegistryUrl);
                }
                catch
                {
                    Console.WriteLine($"ERROR: Schema string was not obtained from \"{schemaRegistryUrl}\".");
                    return null;
                }
            }

            if (string.IsNullOrEmpty(str))
                return null;

            try
            {
                var jOb = JObject.Parse(str.Replace(" ", string.Empty).Replace("\n", "").Replace("\r", "").Replace("\t", "").Replace("\\", ""));
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
