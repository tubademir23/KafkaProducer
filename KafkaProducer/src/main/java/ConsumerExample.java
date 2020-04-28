import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
  ***Before calling producer; zookeeper and kafka must be stand
  *sample for windows 
    @To start zookeeper
	KAFKA_BIN_PATH\windows\zookeeper-server-start.bat .\config\server.properties

	@To start Kafka Broker
	KAFKA_BIN_PATH\windows\kafka-server-start.bat .\config\server.properties

	@To Create topic
	KAFKA_BIN_PATH\windows>kafka-topics.bat --create --zookeeper HOST_NAME:2181 --replication-factor 1 --partitions 1 --topic TOPIC_NAME
	
	@To create producer
	KAFKA_BIN_PATH\windows>kafka-console-producer.bat --broker-list HOST_NAME:PORT --topic TOPIC_NAME

 * */
public class ConsumerExample {
	public static final String TOPIC_NAME = "search";
	public static final String HOST_NAME = "localhost";
	public static final String PORT = "9092";
	public static final String TYPE_NAME = "_doc";

	KafkaConsumer<String, String> consumer;

	public ConsumerExample() {
		Properties configPro = config();
		this.consumer = new KafkaConsumer<String, String>(configPro);
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
	}

	public static Properties config() {
		Properties configPro = new Properties();
		configPro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_NAME + ":" + PORT);
		// !!BE CAREFUL; key and value types: StringDeserializer, enum type:DESERIALIZER

		configPro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		configPro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		configPro.put(ConsumerConfig.GROUP_ID_CONFIG, "SAMPLE_GROUP");
		configPro.put(ConsumerConfig.CLIENT_ID_CONFIG, "SAMPLE_CLIENT");
		return configPro;
	}

	public static void main(String[] args) throws InterruptedException {
		ConsumerExample examp = new ConsumerExample();
		examp.consume();
		examp.consumer.close();

	}

	/*
	 *** The result is same as
	 * 
	 * @Start consumer
	 * 
	 * kafka-console-consumer.bat --bootstrap-server HOST_NAME + ":" + PORT --topic
	 * TOPIC_NAME --from-beginning
	 * 
	 */
	public void consume() {

		try {
			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(2000);

				for (ConsumerRecord<String, String> consumerRecord : records) {
					System.out.println(consumerRecord.value());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}
}
