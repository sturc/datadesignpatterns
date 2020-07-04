package de.dhbw.mosbach.dp.kafka.solutions;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
		String[] animals = { "Snake", "Ape", "Lion", "Spider", "Whale", "Tiger" };
		Properties properties = props.getKafkaConsumerProperties();
		properties.put("group.id", groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		consumer.poll(0);
		consumer.seekToBeginning(consumer.assignment());
		int counter[] = new int[animals.length];
		int emptyPollRequests = 0;
		while (emptyPollRequests < 5) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				String value = record.value();
				for (int i = 0; i < animals.length; ++i) {
					if (animals[i].equals(value)) {
						counter[i]++;
					}
				}

			}
			if (records.isEmpty()) {
				emptyPollRequests++;
			} else {
				emptyPollRequests = 0;
			}

		}
		consumer.close();
		for (int i = 0; i < animals.length; ++i) {
			System.out.println(animals[i] + " " + counter[i]);
		}

	}

}
