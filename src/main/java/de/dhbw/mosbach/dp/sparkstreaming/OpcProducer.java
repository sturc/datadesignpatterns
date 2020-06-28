package de.dhbw.mosbach.dp.sparkstreaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import de.dhbw.mosbach.dp.KafkaProperties;

public class OpcProducer {

	private final static String OPCFile = "src/main/resources/virtual_molding_press_2019-03-25.csv";


	private String topic;
	private KafkaProperties props;

	public static void main(String[] args) {
		OpcProducer c = new OpcProducer();
		c.produce();
	}


	public OpcProducer() {
		this.props = new KafkaProperties();
		this.topic = props.getKafkaTropicPrefix() + "opc";

	}

	public void produce() {

		RecordMetadata metadata;
		try {
			Properties prodProps = this.props.getKafkaProducerProperties();
			prodProps.put("group.id", props.getKafkaUserName() + "-" + this.getClass());
			Producer<String, String> producer = new KafkaProducer<>(prodProps);
			BufferedReader br = null;
			String line = "";
			try {
				br = new BufferedReader(new FileReader(OPCFile));
				while ((line = br.readLine()) != null) {
					ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, line);
					metadata = producer.send(record).get();
					System.out.println("Record sent to partition " + metadata.partition() + " with offset "
							+ metadata.offset() + " with key: " + record.key() + " with the value: " + line);

				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			producer.close();
		} catch (InterruptedException v) {
			System.out.println(v);
		} catch (ExecutionException e) {
			System.out.println(e);
		}
	}


}
