package de.dhbw.mosbach.dp.sparkstreaming;




import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import de.dhbw.mosbach.dp.KafkaProperties;

public class SparkConsumer {

	public static final String APP_NAME = "SparkConsumer-Sturm";

	private static final Pattern SPACE = Pattern.compile(";");

	public static void main(String[] args) throws Exception {

		String master;

		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local[*]";
		}

		KafkaProperties userProps = new KafkaProperties();
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, userProps.getKafkaUserName(), userProps.getKafkaPassword());
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", userProps.getKafkaBrokers());
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", APP_NAME);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		kafkaParams.put("auto.commit.interval.ms", "1000");
		kafkaParams.put("session.timeout.ms", "300000");
		kafkaParams.put("security.protocol", "SASL_SSL");
		kafkaParams.put("sasl.mechanism", "SCRAM-SHA-256");
		kafkaParams.put("sasl.jaas.config", jaasCfg);
		
		Collection<String> topics = Arrays.asList(userProps.getKafkaTropicPrefix() + "opc");


		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(master);

		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2))) {

			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
					jssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
			JavaDStream<String> lines = stream.map(x -> x.value());

			lines.foreachRDD(x -> {
				x.collect().stream().forEach(n -> System.out.println("item of list: " + n));
			});

			// System.out.println("Num Messages: " + lines.count());
//			JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
//			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//					.reduceByKey((i1, i2) -> i1 + i2);
//			wordCounts.print();
//			stream.foreachRDD(rdd -> {
//				OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//			});
//			JavaPairDStream<String, String> pairStream = stream
//					.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
//			System.out.println("Num Messages: " + pairStream.count());
//			pairStream.print();

			// Start the computation
			jssc.start();
			jssc.awaitTermination();
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}
