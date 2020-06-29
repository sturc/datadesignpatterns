package de.dhbw.mosbach.dp.kafka.exercises;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import de.dhbw.mosbach.dp.KafkaProperties;



public class CounterKafkaConsumer {

	private KafkaProperties props;
	private String topic;
	private String groupId;


	public static void main(String[] args) {
		CounterKafkaConsumer c = new CounterKafkaConsumer();
		c.countAnimalMessages();
	}

	public CounterKafkaConsumer() {
		this.props = new KafkaProperties();
		this.topic = props.getKafkaTropicPrefix() + "s0";
		this.groupId = props.getKafkaUserName() + "-" + this.getClass();
	}

	public void countAnimalMessages() {
		Properties properties = props.getKafkaConsumerProperties();
		properties.put("group.id", groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {

			// TODO implement

		}
	}

}
