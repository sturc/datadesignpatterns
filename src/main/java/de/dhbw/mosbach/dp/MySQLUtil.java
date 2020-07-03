package de.dhbw.mosbach.dp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLUtil {

	private static String url = "jdbc:mysql://localhost:3306/mysqldp";
	private static String user = "mysql";
	private static String pwd = "mysql";

	private MySQLUtil() {
	}


	// 1. Establish a connection
	public static Connection getConnection() throws SQLException {
		Connection connection = DriverManager.getConnection(url, user, pwd);
		return connection;
	}

	// 2. Close resources
	public static void free(Statement stmt, Connection conn) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// 3. Close resource 2
	public static void free2(ResultSet rs, Statement stmt, Connection conn) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}