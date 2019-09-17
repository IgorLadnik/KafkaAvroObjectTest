This is a simple test for producing / consuming Avro GenericRecord object with Kafka.
To use the code some prerequisites are required.
You need to use Zookeeper and Kafka.
I installed both of them locally on my Windows machine.
Installation guidelines may be found here: http://programming-tips.in/kafka-set-up-apache-kafka-on-windows/
After installation my 
Zookeeper directory is C:\zookeeper-3.5.5 , and my 
Kafka directory is C:\kafka_2.12-2.3.0

Main() in Program.cs performs the following:

- defines configuration parameters to access Kafka,
- obtains Record Schema String from some Schema Registry with HTTP GET request and parses the string to Record Schema,
- creates KafkaConsumer and call its StartConsuming() method,
- creates GenericRecord object using Record Schema,
- produces newly created object to Kafka with string key, and
- consumes all objects from Kafka.

Difference with usual implementation of Producer and Consumer is in usage of ISchemaRegistryClient interface 
in Avro serializer and deserializer.
Usually class CachedSchemaRegistryClient : ISchemaRegistryClient is used.
This class itself connects to Schema Registry with HTTP POST to obtain Record Schema.
It requires more complex Schema Registry service that can serve such requests.
In contrast in this code Record Schema String is obtained separately and 
a new class SchemaRegistryClient provides simple implementation of ISchemaRegistryClient interface
based on already available Record Schema.

It is convenient to produce / consume GenericRecord objects based on Record Schemause Record Schema.
E.g., versions backward and forward compatibility may be kept.
But each such object contains Schema String and therefore is quite heavy.

To run the code you need to start first Zookeeper, Kafka and Schema Registry service.
First, Zookeeper is running with command file _start\z.cmd .
Then Kafka is running with command file _start\k.cmd .

Schema Registry service is used as a local Web Directory unsed IISExpress.
Appropriate site is added to C:\Users\Igorl\Documents\IISExpress\config\applicationhost.config file
within <sites> tag:

<site name="SchemaTest" id="19">
	<application path="/" applicationPool="Clr4IntegratedAppPool">
		<virtualDirectory path="/" physicalPath="C:\prj\KafkaAvroObjectTest\wwwroot" />				
	</application>
	<bindings>
		<binding protocol="http" bindingInformation=":9797:localhost" />
	</bindings>
</site>

where C:\prj\KafkaAvroObjectTest\wwwroot directory contains file schema.html containing actual JSON Schema String.
Site SchemaTest is activating by running command file _site\startIIS.cmd .





