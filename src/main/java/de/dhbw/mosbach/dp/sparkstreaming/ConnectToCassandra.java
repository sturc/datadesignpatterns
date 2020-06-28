package de.dhbw.mosbach.dp.sparkstreaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.japi.CassandraJavaUtil;


public class ConnectToCassandra {

	public static final String APP_NAME = "SparkScrucuredStreaming-Sturm";
	
	public static void main(String[] args) {
		String master;

		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local[2]";
		}

		SparkSession spark = SparkSession.builder().master(master).appName("APP_NAME")
				.config("spark.cassandra.connection.config.cloud.path",
						"file:///Users/sturm/Development/spark-streaming/secure-connect-wimos.zip")
				.config("spark.cassandra.auth.username", "dbuser")
				.config("spark.cassandra.auth.password", "dpss20")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

//		Map<String, String> tableProperties = new HashMap();
//		tableProperties.put("keyspace", "data");
//		tableProperties.put("table", "wordcount");
//		tableProperties.put("confirm.truncate", "true");
		JavaRDD<Integer> cassandraRdd = CassandraJavaUtil.javaFunctions(spark.sparkContext())
				.cassandraTable("data", "wordcount",
						com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo(Integer.class))
				.select("counts");
		System.out.println(cassandraRdd.collect());
		
		Dataset<Row> df = spark.read().format("org.apache.spark.sql.cassandra").option("keyspace", "data")
				.option("table", "wordcount").load();
		df.show();
		
		String createDDL = "CREATE TEMPORARY VIEW wordcount USING org.apache.spark.sql.cassandra OPTIONS (table \"wordcount\", keyspace \"data\", cluster \"wimos\", pushdown \"true\")";
		spark.sql(createDDL); // Creates Catalog Entry registering an existing Cassandra Table
		spark.sql("SELECT * FROM wordcount").show();
		spark.sql("SELECT * FROM wordcount WHERE word = 'test'").show();
		
		df.write().format("org.apache.spark.sql.cassandra").option("keyspace", "data")
				.option("table", "wordcountcopy")
				.save();
	

		// rest
		// https://ceb3bcf2-77a6-4398-b42a-4f81f5972f6a-europe-west1.apps.astra.datastax.com/api/rest

		// Create the CqlSession object:
//		try (CqlSession session = CqlSession.builder()
//				.withCloudSecureConnectBundle(
//						Paths.get("/Users/sturm/Development/spark-streaming/secure-connect-wimos.zip"))
//				.withAuthCredentials("dbuser", "dpss20").withKeyspace("data").build()) {
//			// Select the release_version from the system.local table:
//			ResultSet rs = session.execute("select * from data.wordcount");
//			Row row = rs.one();
//			// Print the results of the CQL query to the console:
//			if (row != null) {
//				System.out.println(row.getString("word"));
//			} else {
//				System.out.println("An error occurred.");
//			}
//		}
		System.exit(0);
	}

}
