package de.dhbw.mosbach.dp.sparkstreaming;





import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import de.dhbw.mosbach.dp.KafkaProperties;
import de.dhbw.mosbach.dp.MySQLUtil;

public class SparkStructuredStreamingWithCassandra {

	public static final String APP_NAME = "SparkScrucuredStreaming-Sturm";

	private static final Pattern SPACE = Pattern.compile(";");

	public static void main(String[] args) throws Exception {

		String master;

		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local[*]";
		}

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Database driver loading failed!");
		}

		KafkaProperties userProps = new KafkaProperties();
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, userProps.getKafkaUserName(), userProps.getKafkaPassword());
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("kafka.bootstrap.servers", userProps.getKafkaBrokers());
		kafkaParams.put("kafka.group.id", APP_NAME);
		kafkaParams.put("startingOffsets", "earliest");
		// kafkaParams.put("auto.commit.interval.ms", "1000");
		kafkaParams.put("kafka.session.timeout.ms", "30000");
		kafkaParams.put("kafka.security.protocol", "SASL_SSL");
		kafkaParams.put("kafka.sasl.mechanism", "SCRAM-SHA-256");
		kafkaParams.put("kafka.sasl.jaas.config", jaasCfg);
		
		String topic = userProps.getKafkaTropicPrefix() + "opc";


		// Initialize Spark Context
		SparkSession spark = SparkSession.builder().master(master).appName("APP_NAME")
				.config("spark.cassandra.connection.config.cloud.path",
						"file:///Users/sturm/Development/spark-streaming/secure-connect-wimos.zip")
				.config("spark.cassandra.auth.username", "dbuser").config("spark.cassandra.auth.password", "dpss20")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

//		Dataset<String> lines = spark.readStream().format("kafka").options(kafkaParams)
//				.option("subscribe", topic)
//				.load()
//				.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

		Dataset<Row> rows = spark.readStream().format("kafka").options(kafkaParams).option("subscribe", topic)
				.load()
				.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG)");

		// Generate running word count
		
		StructType opcSchema = new StructType(
				new StructField[] {
				new StructField("NS", DataTypes.StringType, false, Metadata.empty()),
						new StructField("SensorID", DataTypes.StringType, false, Metadata.empty()),
						new StructField("Value", DataTypes.StringType, false, Metadata.empty()),
						new StructField("Timestamp", DataTypes.TimestampType, false, Metadata.empty()) });
		rows.printSchema();
		Encoder<Row> encoder = RowEncoder.apply(opcSchema);
				
		Dataset<Row> structRows = rows.map(new MapFunction<Row, Row>() {

			private static final long serialVersionUID = 445454;
			private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			@Override
			public Row call(Row record) throws Exception {
				String[] attributes = ((String) record.getAs("value")).split(";");
				if (attributes.length == 1) {
					System.out.println((String) record.getAs("value"));
					return RowFactory.create(attributes[0], "", "", new Timestamp(0));
				} else if (attributes.length == 2) {
					System.out.println((String) record.getAs("value"));
					return RowFactory.create(attributes[0], attributes[1], "", new Timestamp(0));
				} else {
					java.util.Date parsedDate = dateFormat.parse(attributes[3]);
					return RowFactory.create(attributes[0], attributes[1], attributes[2],
							new Timestamp(parsedDate.getTime()));
				}
			}
		},encoder);

		structRows.printSchema();

		// do some aggregation and processing
		// Generate running word count
		Dataset<Row> rowsWithNumValues = structRows.filter(new FilterFunction<Row>() {
			private static final long serialVersionUID = 1L;

			public boolean call(Row row) {
				if (isNumeric(row.getAs(2))) {
					return true;
				} else {
					return false;
				}
			}

		}).withColumn("valuetmp", structRows.col("Value").cast(DataTypes.IntegerType))
				.drop("Value")
				.withColumnRenamed("valuetmp", "Value");

		Dataset<Row> avgNums = rowsWithNumValues.withWatermark("Timestamp",
				"20 Minutes")
				.groupBy(
				"NS",
				"SensorID")
				.avg("Value").withColumnRenamed("avg(Value)", "AvgValue");
		avgNums.printSchema();
		Dataset<Row> avgNumsWithTimestamp = avgNums.withColumn("Timestamp",
				functions.lit(new Timestamp(new Date().getTime())));
		avgNumsWithTimestamp.printSchema();
		// file sink
		/*StreamingQuery query = avgNums.writeStream()
				.format(
				"csv")
				.option("checkpointLocation", "/tmp/avgchkpoint_dir").option("path",
						"/tmp/avgresult")
				.outputMode("append").trigger(Trigger.ProcessingTime("5 seconds")).start();
*/

	  


		// Start running the query that prints the running counts to the console
		// in memory solution
		/*
		 * StreamingQuery query = wordCounts.writeStream().queryName( "resulttable") //
		 * this query name // will be the table // name
		 * .outputMode("complete").format("memory").trigger(Trigger.
		 * ProcessingTime("5 seconds")).start();
		 * spark.sql("select * from resulttable").show(); // interactively query
		 * in-memory table
		 */		

		// cassandra solution
		/*
		 * StreamingQuery query = wordCounts.withColumnRenamed("value",
		 * "word").withColumnRenamed("count", "counts") .writeStream().outputMode(
		 * "complete") .format("org.apache.spark.sql.cassandra").option("keyspace",
		 * "data") .option("table", "wordcount") .option("checkpointLocation",
		 * "/tmp/checkpoint") .trigger(Trigger.ProcessingTime("5 seconds")).start();
		 */
		
		// mysql solution (does not work with continous queries)
		/*
		 * StreamingQuery query = avgNumsWithTimestamp.withColumn("valuetmp",
		 * avgNums.col( "AvgValue") .cast( DataTypes.StringType))
		 * .drop("AvgValue").withColumnRenamed("valuetmp", "Value")
		 * .withWatermark("Timestamp", "20 Minutes") .writeStream().outputMode(
		 * "complete") .foreachBatch( new VoidFunction2<Dataset<Row>, Long>() {
		 * 
		 * private static final long serialVersionUID = 1L;
		 * 
		 * public void call(Dataset<Row> dataset, Long batchId) {
		 * dataset.write().format("jdbc").option("url", "jdbc:mysql:db4free.net:3306")
		 * .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "opcdata")
		 * .option("user", "dpuser") .option("password", "DesignPatternsSS20")
		 * .mode("append").save(); } })
		 * .trigger(Trigger.ProcessingTime("5 seconds")).start();
		 */
		
		// mysql foreach solution 
		StreamingQuery query = avgNumsWithTimestamp.withColumn("valuetmp",
				avgNums.col("AvgValue").cast(DataTypes.StringType)).drop("AvgValue")
				.withColumnRenamed("valuetmp", "Value").writeStream()
				.outputMode(
						"complete")
				.foreach(new ForeachWriter<Row>() {

					private static final long serialVersionUID = 1L;
					private Connection conn;
					private PreparedStatement pstmt;

					@Override
					public boolean open(long partitionId, long version) {
						try {
							conn = MySQLUtil.getConnection();
							String sql = "insert into opcdata (NS,SensorID,Value,Timestamp) values(?,?,?,?)";
							pstmt = conn.prepareStatement(sql);
						} catch (SQLException e) {
							e.printStackTrace();
						}
						return (conn != null);
					}

					@Override
					public void process(Row record) {
						try {
							pstmt.setString(1, record.getAs("NS"));
							pstmt.setString(2, record.getAs("SensorID"));
							pstmt.setString(3, record.getAs("Value"));
							pstmt.setTimestamp(4, record.getAs("Timestamp"));
							pstmt.executeUpdate();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					@Override
					public void close(Throwable errorOrNull) {
								MySQLUtil.free(pstmt, conn);
					}
				}).trigger(Trigger.ProcessingTime("5 seconds")).start();


//		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console")
//				.trigger(Trigger.ProcessingTime("5 seconds")).start();


		query.awaitTermination();


	}

	public static boolean isNumeric(String strNum) {
		if (strNum == null) {
			return false;
		}
		try {
			double d = Double.parseDouble(strNum);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
}
