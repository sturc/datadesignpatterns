package de.dhbw.mosbach.dp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProperties {

	private final static String PROPFILE = "/Users/sturm/Development/spark-streaming/ldvj1amy-sasl-scram.txt";
	private Properties userProps = new Properties();
	private final Properties kafkaProps = new Properties();

	public KafkaProperties() {
		try (InputStream input = new FileInputStream(PROPFILE)) {
			userProps.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, userProps.getProperty("CLOUDKARAFKA_USERNAME"),
				userProps.getProperty("CLOUDKARAFKA_PASSWORD"));
		kafkaProps.put("bootstrap.servers", userProps.getProperty("CLOUDKARAFKA_BROKERS"));
		kafkaProps.put("security.protocol", "SASL_SSL");
		kafkaProps.put("sasl.mechanism", "SCRAM-SHA-256");
		kafkaProps.put("sasl.jaas.config", jaasCfg);
	}

	public Properties getKafkaProperties() {
		return kafkaProps;

	}

	public Properties getKafkaProducerProperties() {
		String serializer = StringSerializer.class.getName();
		Properties prodProperties = (Properties) kafkaProps.clone();
		prodProperties.put("key.serializer", serializer);
		prodProperties.put("value.serializer", serializer);
		prodProperties.setProperty("acks", "all");
		return prodProperties;
	}

	public Properties getKafkaConsumerProperties() {
		String deserializer = StringDeserializer.class.getName();
		Properties consumerProperties = (Properties) kafkaProps.clone();
		consumerProperties.put("key.deserializer", deserializer);
		consumerProperties.put("value.deserializer", deserializer);
		consumerProperties.put("enable.auto.commit", "true");
		consumerProperties.put("auto.commit.interval.ms", "1000");
		consumerProperties.put("auto.offset.reset", "earliest");
		consumerProperties.put("session.timeout.ms", "30000");
		return consumerProperties;
	}

	public String getKafkaBrokers() {
		return userProps.getProperty("CLOUDKARAFKA_BROKERS");
	}

	public String getKafkaUserName() {
		return userProps.getProperty("CLOUDKARAFKA_USERNAME");
	}

	public String getKafkaPassword() {
		return userProps.getProperty("CLOUDKARAFKA_PASSWORD");
	}

	public String getKafkaTropicPrefix() {
		return userProps.getProperty("CLOUDKARAFKA_TOPIC_PREFIX");
	}




}