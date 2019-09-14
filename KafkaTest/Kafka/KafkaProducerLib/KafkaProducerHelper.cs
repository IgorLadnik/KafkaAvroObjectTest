using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Collections.Concurrent;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using KafkaCommonLib;

namespace KafkaProducerLib
{
    public class KafkaProducer
    {
        #region Vars

        private ConcurrentQueue<KeyValuePair<string, GenericRecord>> _cquePair = new ConcurrentQueue<KeyValuePair<string, GenericRecord>>();

        private IProducer<string, GenericRecord> _producer;
        private Task _task;
        private bool _isClosing = false;
        private string _topicName;
        private long _isSending = 0;
        
        public string Error { get; private set; }

        #endregion // Vars

        #region Ctor

        public KafkaProducer(string bootstrapServers,
                             string schemaString, //1 string schemaRegistryUrl,
                             string topic,
                             string subject,
                             int version,
                             int id, 
                             int partition,
                             int offset,
                             Action<string> errorHandler)
        {
            if (errorHandler == null)
                throw new Exception("Empty handler");

            //1 var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            //{
            //    SchemaRegistryUrl = schemaRegistryUrl,
            //    SchemaRegistryRequestTimeoutMs = 5000,
            //});
            var schemaRegistry = new SchemaRegistryClient(new Schema(subject, version, id, schemaString)); //1

            _producer =
                new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                    .Build();

            _topicName = topic;
        }

        #endregion // Ctor

        #region Send Methods 

        public KafkaProducer Send(string key, GenericRecord value)
        {
            if (string.IsNullOrEmpty(key) || value == null)
                return this;

            return Send(new KeyValuePair<string, GenericRecord>(key, value));
        }

        public KafkaProducer Send(params Tuple<string, GenericRecord>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;

            var lst = new List<KeyValuePair<string, GenericRecord>>();
            foreach (var tuple in arr)
                lst.Add(new KeyValuePair<string, GenericRecord>(tuple.Item1, tuple.Item2));

            return Send(lst.ToArray());
        }

        public KafkaProducer Send(params KeyValuePair<string, GenericRecord>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;
            
            if (!_isClosing)
                foreach (var pair in arr)
                    _cquePair.Enqueue(new KeyValuePair<string, GenericRecord>(pair.Key, pair.Value));

            if (Interlocked.Read(ref _isSending) == 1)
                return this;

            _task = Task.Run(async () =>
            {
                DeliveryResult<string, GenericRecord> dr = null;

                Interlocked.Increment(ref _isSending);

                while (_cquePair.TryDequeue(out var pair))
                {
                    try
                    {
                        dr = await _producer.ProduceAsync(_topicName, new Message<string, GenericRecord> { Key = pair.Key, Value = pair.Value });
                            //.ContinueWith(task => task.IsFaulted
                            //         ? $"error producing message: {task.Exception.Message}"
                            //         : $"produced to: {task.Result.TopicPartitionOffset}")
                            //.Wait();
                    }
                    catch (Exception e)
                    {
                        //Console.WriteLine($"Send failed: {e}");
                        Error = e.Message;
                        break;
                    }
                }

                Interlocked.Decrement(ref _isSending);
            });

            return this;
        }

        #endregion // Send Methods

        #region Dispose 

        public void Dispose()
        {
            _isClosing = true;
            _cquePair = null;

            _task?.Wait();

            _producer?.Dispose();
        }

        #endregion // Dispose 
    }
}
