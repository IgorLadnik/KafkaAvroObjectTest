This is a simple test for producing / consuming Avro objects with Kafka. To use the code some prerequisites are required. You need to use Zookeeper and Kafka. I installed both of them locally on my Windows machine. Installation guidelines may be found here: http://programming-tips.in/kafka-set-up-apache-kafka-on-windows/ After installation my Zookeeper directory is C:\zookeeper-3.5.5 , and my Kafka directory is C:\kafka_2.12-2.3.0

Main() in Program.cs performs the following:

defines configuration parameters (such as brockers, topic, partition, Schema Registry URL) to access Kafka,
obtains Record Schema String from some Schema Registry service with HTTP GET request and parses the string to Record Schema,
creates KafkaConsumer and call its StartConsuming() method,
creates objects to be stored (either of GenericRecord type or of a type generated by AvroGen tool) using Record Schema,
produces newly created objects to Kafka with string keys, and
consumes all objects from Kafka.
Difference with usual implementation of Producer and Consumer is in usage of ISchemaRegistryClient interface in Avro serializer and deserializer. Usually class CachedSchemaRegistryClient : ISchemaRegistryClient is used. This class itself connects to Schema Registry with HTTP POST to obtain Record Schema. It requires more complex Schema Registry service that can serve such requests. In contrast in this code Record Schema String is obtained separately and a new class SchemaRegistryClient provides simple implementation of ISchemaRegistryClient interface based on already available Record Schema.

It is convenient to produce / consume objects of types generated according to Record Schemause Record Schema. This ensure several advantages, e. g., versions backward and forward compatibility may be kept.

This code allows developer to produce / consume object of either

standard GenericRecord type (#define GENERIC_RECORD), or
specific types generated by AvroGen tool using appropriate schema (//#define GENERIC_RECORD). Class cache_youtube_category_mapping provides example of the latter approach. Appropriate file was generated by the following console command:
...\KafkaAvroObjectTest\AvroGen>AvroGen -s _schema.json .

It generated file ...\KafkaAvroObjectTest\AvroGen\com\dv\cache_youtube_category_mapping.cs with path according to namespace in schema.

To run the code you need to start first Zookeeper, Kafka and Schema Registry service. First, Zookeeper is running with command file _start\z.cmd . Then Kafka is running with command file _start\k.cmd .

Schema Registry service is used as a local Web Directory unsed IISExpress. Appropriate site is added to C:\Users\Igorl\Documents\IISExpress\config\applicationhost.config file within tag:

where C:\prj\KafkaAvroObjectTest\wwwroot directory contains file schema.html containing actual JSON Schema String. Site SchemaTest is activating by running command file _site\startIIS.cmd .
