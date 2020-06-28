package de.dhbw.mosbach.dp.sparkstreaming;

import java.nio.file.Paths;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class JavaConnectToCassandra {

	public static void main(String[] args) {
		// Create the CqlSession object:
		try (CqlSession session = CqlSession.builder()
				.withCloudSecureConnectBundle(
						Paths.get("/Users/sturm/Development/spark-streaming/secure-connect-wimos.zip"))
				.withAuthCredentials("dbuser", "dpss20").withKeyspace("data").build()) {
			// Select the release_version from the system.local table:
			ResultSet rs = session.execute("select * from data.wordcount");
			Row row = rs.one();
			// Print the results of the CQL query to the console:
			if (row != null) {
				System.out.println(row.getString("word"));
			} else {
				System.out.println("An error occurred.");
			}
		}
		System.exit(0);
	}

}
