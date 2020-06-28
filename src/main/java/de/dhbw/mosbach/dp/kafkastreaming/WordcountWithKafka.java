package de.dhbw.mosbach.dp.kafkastreaming;

public class WordcountWithKafka {

	public static void main(String[] args) {
		/*
		 * // TODO Auto-generated method stub KafkaProperties kafProp = new
		 * KafkaProperties(); Properties kafkaProperties = kafProp.getKafkaProperties();
		 * kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
		 * 
		 * KStreamBuilder builder = new KStreamBuilder();
		 * 
		 * KStream<String, String> source = builder.stream(
		 * kafProp.getKafkaTropicPrefix() + "s0");
		 * 
		 * 
		 * final Pattern pattern = Pattern.compile("\\W+");
		 */

		/*
		 * KStream counts = source.flatMapValues(value ->
		 * Arrays.asList(pattern.split(value.toLowerCase()))) .map((key, value) -> new
		 * KeyValue<Object, Object>(value, value)) .filter((key, value) ->
		 * (!value.equals("the"))).groupByKey().count("CountStore") .mapValues(value ->
		 * Long.toString(value)).toStream(); counts.to("wordcount-output");
		 */
		
	}

}
