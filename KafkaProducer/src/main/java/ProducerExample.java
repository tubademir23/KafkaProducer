import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
  ***Before calling producer; zookeeper and kafka must be stand
  *sample for windows 
    @To start zookeeper
	KAFKA_BIN_PATH\windows\zookeeper-server-start.bat .\config\server.properties

	@To start Kafka Broker
	KAFKA_BIN_PATH\windows\kafka-server-start.bat .\config\server.properties

	@To Create topic
	KAFKA_BIN_PATH\windows>kafka-topics.bat --create --zookeeper HOST_NAME:2181 --replication-factor 1 --partitions 1 --topic TOPIC_NAME
	
	@To create consumer
	KAFKA_BIN_PATH\windows>kafka-console-consumer.bat --bootstrap-server HOST_NAME:PORT --topic TOPIC_NAME

 * */
public class ProducerExample {
	public static final String TOPIC_NAME = "search";
	public static final String HOST_NAME = "localhost";
	public static final String PORT = "9092";
	public static final String TYPE_NAME = "_doc";

	Producer producer;

	public ProducerExample() {
		Properties configPro = config();
		this.producer = new KafkaProducer<String, String>(configPro);
	}

	public static Properties config() {
		Properties configPro = new Properties();
		configPro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_NAME + ":" + PORT);
		// string values,
		configPro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");

		configPro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		return configPro;
	}

	public static void main(String[] args) throws InterruptedException {
		ProducerExample examp = new ProducerExample();

		String[] samples = { "java", "c#", "python" };
		for (String text : samples) {
			examp.produce(text);
			TimeUnit.SECONDS.sleep(2);
		}
		examp.producer.close();

	}

	/*
	 *** The result is same as
	 * 
	 * @Start Producer
	 * 
	 * @C:\kafka_2.12-2.5.0\bin\windows>kafka-console-producer.bat --broker-list
	 * localhost:9092 --topic test
	 * 
	 */
	public void produce(String text) {

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, text);

		producer.send(record);
	}
}
