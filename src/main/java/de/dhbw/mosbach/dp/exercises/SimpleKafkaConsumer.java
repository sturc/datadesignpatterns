package de.dhbw.mosbach.dp.exercises;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import de.dhbw.mosbach.dp.KafkaProperties;




public class SimpleKafkaConsumer {

	private KafkaProperties props;
	private String topic;
	private String groupId;


	public static void main(String[] args) {
		SimpleKafkaConsumer c = new SimpleKafkaConsumer();
		c.consume();
		// c.consumePartition();
	}

	public SimpleKafkaConsumer() {
		this.props = new KafkaProperties();
		this.topic = props.getKafkaTropicPrefix() + "s0";
		this.groupId = props.getKafkaUserName() + "-" + this.getClass();
	}

	public void consume() {
		Properties kafkaProperties = props.getKafkaConsumerProperties();
		kafkaProperties.put("group.id", groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
		consumer.subscribe(Arrays.asList(topic));
		// TODO adjust the consumer (offset, ...)
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n", record.topic(), record.partition(),
						record.offset(), record.key(), record.value());
			}
		}
	}

	// TODO write a consumer for a specific partition
	// TODO test the different seekToBeginning(partitions)
	public void consumeFromBeginning() {
		Properties kafkaProperties = props.getKafkaConsumerProperties();
		kafkaProperties.put("group.id", groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
		consumer.subscribe(Arrays.asList(topic));
		consumer.poll(0);
		consumer.seekToBeginning(consumer.assignment());
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		System.out.println(partitions);
		while (true) {
			// TODO add implementation
		}

	}
}
