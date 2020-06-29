package de.dhbw.mosbach.dp.sparkstreaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConnectToKafka {

	private final static String PROPFILE = "../ldvj1amy-sasl-scram.txt";

	private final String topic;
	private final Properties props;
	private static Properties userProps;

	private static void initPropsFromFile() {
		try (InputStream input = new FileInputStream(PROPFILE)) {

			userProps = new Properties();

			// load a properties file
			userProps.load(input);

			// get the property value and print it out
			System.out.println(userProps.getProperty("CLOUDKARAFKA_BROKERS"));
			System.out.println(userProps.getProperty("CLOUDKARAFKA_USERNAME"));
			System.out.println(userProps.getProperty("CLOUDKARAFKA_PASSWORD"));
			System.out.println(userProps.getProperty("CLOUDKARAFKA_TOPIC_PREFIX"));

		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	public static void main(String[] args) {
		ConnectToKafka c = new ConnectToKafka();
		// c.produce();
		c.consume();
	}

	public ConnectToKafka() {
		initPropsFromFile();

		this.topic = userProps.getProperty("CLOUDKARAFKA_TOPIC_PREFIX") + "opc";

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, userProps.getProperty("CLOUDKARAFKA_USERNAME"),
				userProps.getProperty("CLOUDKARAFKA_PASSWORD"));

        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        props = new Properties();
		props.put("bootstrap.servers", userProps.getProperty("CLOUDKARAFKA_BROKERS"));
		props.put("group.id", userProps.getProperty("CLOUDKARAFKA_USERNAME") + "-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);
    }

	public void produce() {
		Thread one = new Thread() {
			public void run() {
				RecordMetadata metadata;
				try {
					Producer<String, String> producer = new KafkaProducer<>(props);
					String[] animals = { "Schlange", "Affe", "Löwe", "Spinne", "Wal" };
					for (int i = 0; i <= 500; i++) {
						Random r = new Random();
						int low = 0;
						int high = 5;
						int resultRandom = r.nextInt(high - low) + low;
						String msg = animals[resultRandom];
						System.out.println(animals[resultRandom]);
						ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
						metadata = producer.send(record).get();
						System.out.println("Record sent to partition " + metadata.partition() + " with offset "
								+ metadata.offset() + " with key: " + record.key() + " with the value: " + msg);
					}
					producer.close();
				} catch (InterruptedException v) {
					System.out.println(v);
				} catch (ExecutionException e) {
					System.out.println(e);
				}
			}

		};
		one.start();
	}

	public void consume() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n", record.topic(), record.partition(),
						record.offset(), record.key(), record.value());
			}
		}
	}

}
