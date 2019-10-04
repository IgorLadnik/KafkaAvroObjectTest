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

        private ConcurrentQueue<KeyValuePair<string, byte[]>> _cquePair = new ConcurrentQueue<KeyValuePair<string, byte[]>>();

        private IProducer<string, byte[]> _producer;
        private Task _task;
        private bool _isClosing = false;
        private string _topicName;
        private long _isSending = 0;
        
        public string Error { get; private set; }

        #endregion // Vars

        #region Ctor

        public KafkaProducer(string bootstrapServers,
                             RecordConfig recordConfig,
                             string topic,
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
            var schemaRegistry = new SchemaRegistryClient(new Schema(recordConfig.Subject, recordConfig.Version, recordConfig.Id, recordConfig.SchemaString)); //1

            _producer =
                new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(Serializers.ByteArray/*new AvroSerializer<T>(schemaRegistry)*/)
                    .Build();

            _topicName = topic;
        }

        #endregion // Ctor

        #region Send Methods 

        public KafkaProducer Send(string key, byte[] value)
        {
            if (string.IsNullOrEmpty(key) || value == null)
                return this;

            return Send(new KeyValuePair<string, byte[]>(key, value));
        }

        public KafkaProducer Send(params Tuple<string, byte[]>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;

            var lst = new List<KeyValuePair<string, byte[]>>();
            foreach (var tuple in arr)
                lst.Add(new KeyValuePair<string, byte[]>(tuple.Item1, tuple.Item2));

            return Send(lst.ToArray());
        }

        public KafkaProducer Send(params KeyValuePair<string, byte[]>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;
            
            if (!_isClosing)
                foreach (var pair in arr)
                    _cquePair.Enqueue(new KeyValuePair<string, byte[]>(pair.Key, pair.Value));

            if (Interlocked.Read(ref _isSending) == 1)
                return this;

            _task = Task.Run(async () =>
            {
                DeliveryResult<string, byte[]> dr = null;

                Interlocked.Increment(ref _isSending);

                while (_cquePair.TryDequeue(out var pair))
                {
                    try
                    {
                        dr = await _producer.ProduceAsync(_topicName, new Message<string, byte[]> { Key = pair.Key, Value = pair.Value });
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
