using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Avro.Generic;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using KafkaCommonLib;
using System.Threading.Tasks;

namespace KafkaConsumerLib
{ 
    public class KafkaConsumer
    {
        #region Vars

        private IConsumer <string, byte[]> _consumer;
        private CancellationTokenSource _cts;
        private AvroDeserializer<GenericRecord> _avroDeserializer;
        private Action<string, GenericRecord, DateTime> _consumeResultHandler;
        private Thread _thread;
        private string _topic;

        public string Error { get; private set; }

        #endregion // Vars

        #region Ctor

        public KafkaConsumer(string bootstrapServers,
                             RecordConfig recordConfig,
                             string topic,
                             string groupId,
                             int partition,
                             int offset,
                             Action<string, dynamic, DateTime> consumeResultHandler,
                             Action<string> errorHandler)
        {
            if (consumeResultHandler == null || errorHandler == null)
                throw new Exception("Empty handler");

            _consumeResultHandler = consumeResultHandler;

            _cts = new CancellationTokenSource();

            //1 var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl });
            //var schemaRegistry = new SchemaRegistryClient(new Schema(recordConfig.Subject, recordConfig.Version, recordConfig.Id, recordConfig.SchemaString)); //1

            var schemaRegistry = new SchemaRegistryClient(new Schema(recordConfig.Subject, recordConfig.Version, recordConfig.Id, recordConfig.SchemaString)); //1
            _avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry);

            _consumer = new ConsumerBuilder<string, byte[]>(
                    new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest })
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(Deserializers.ByteArray/*new AvroDeserializer<T>(schemaRegistry).AsSyncOverAsync()*/)
                    .SetErrorHandler((_, e) => errorHandler(e.Reason))
                    .Build();

            _consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, partition, offset) });

            _topic = topic;
        }

        #endregion // Ctor

        #region Deserialize

        public async Task<GenericRecord> DeserializeAsync(byte[] bts, string topic) =>
           await _avroDeserializer.DeserializeAsync(bts, false, new SerializationContext(MessageComponentType.Value, topic));

        #endregion // Deserialize

        #region StartConsuming Method

        public KafkaConsumer StartConsuming()
        {
            Error = null;

            _thread = new Thread(async () =>
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var cr = _consumer.Consume(_cts.Token);
                        _consumeResultHandler(cr.Key, await DeserializeAsync(cr.Value, _topic), cr.Timestamp.UtcDateTime);
                    }
                }
                catch (ConsumeException e)
                {
                    //Console.WriteLine($"Consuming failed: {e.Error.Reason}");
                    Error = e.Message;
                }
                catch (OperationCanceledException e)
                {
                    Error = e.Message;
                }
                catch (AccessViolationException e)
                {
                    Error = e.Message;
                }
                catch (Exception e)
                {
                    Error = e.Message;
                }
                finally
                {
                    _consumer.Close();
                }
            });
            _thread.Start();

            return this;
        }

        #endregion // StartConsuming Method

        #region Dispose 

        public void Dispose()
        {
            _cts.Cancel();
            if (_thread != null)
                _thread.Join();
        }

        #endregion // Dispose
    }
}
