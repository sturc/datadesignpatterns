package de.dhbw.mosbach.dp.kafka.solutions;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import de.dhbw.mosbach.dp.KafkaProperties;



public class SimpleKafkaProducer {

	private KafkaProperties props;
	private String topic;



	public static void main(String[] args) throws InterruptedException, ExecutionException {
		SimpleKafkaProducer p = new SimpleKafkaProducer();
		p.produce();
		p.produceTigers();
	}

	public SimpleKafkaProducer() {
		this.props = new KafkaProperties();
		this.topic = props.getKafkaTropicPrefix() + "s0";
	}

	public void produce() {
		RecordMetadata metadata;
		try {
			Properties prodProps = this.props.getKafkaProducerProperties();
			prodProps.put("group.id", props.getKafkaUserName() + "-" + this.getClass());
			Producer<String, String> producer = new KafkaProducer<>(prodProps);
			String[] animals = { "Snake", "Ape", "Lion", "Spider", "Whale" };
			for (int i = 0; i <= 5; i++) {
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

	public void produceTigers() throws InterruptedException, ExecutionException {
		Properties prodProps = this.props.getKafkaProducerProperties();
		Producer<String, String> producer = new KafkaProducer<>(prodProps);
		for (int i = 0; i <= 5; i++) {
			String msg = "Tiger";
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
			producer.send(record).get();
		}
		producer.close();
	}
}
