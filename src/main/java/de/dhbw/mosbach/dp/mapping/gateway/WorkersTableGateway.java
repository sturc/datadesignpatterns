package de.dhbw.mosbach.dp.mapping.gateway;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.dhbw.mosbach.dp.MySQLUtil;

public class WorkersTableGateway {

	private Connection conn;

	public WorkersTableGateway() {
		try {
			conn = MySQLUtil.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ResultSet findAll() throws SQLException {
		String sql = "SELECT * FROM Workers";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		return pstmt.executeQuery();
	}

	public ResultSet find(String id) throws SQLException {
		String sql = "SELECT * FROM Workers WHERE id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		return pstmt.executeQuery();
	}

	public ResultSet findWithLastName(String lastName) throws SQLException {
		String sql = "SELECT * FROM Workers WHERE lastname = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, lastName);
		return pstmt.executeQuery();
	}

	public void update(String id, String firstname, String lastname, int department) throws SQLException {
		String sql = "UPDATE Workers SET LastName = ?, FirstName = ?, Dep = ? WHERE ID = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, firstname);
		pstmt.setString(2, lastname);
		pstmt.setInt(3, department);
		pstmt.setString(4, id);
		pstmt.executeUpdate();
	}

	public int insert(String id, String firstname, String lastname, int department) throws SQLException {
		String sql = "insert into Workers (ID,FirstName,LastName,Dep) values(?,?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		pstmt.setString(2, firstname);
		pstmt.setString(3, lastname);
		pstmt.setInt(4, department);
		return pstmt.executeUpdate();
	}

	public void delete(String id) throws SQLException {
		String sql = "DELETE  FROM Workers WHERE id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		pstmt.executeUpdate();
	}

	@Override
	protected void finalize() throws Throwable {
		conn.close();
		super.finalize();
	}

	public static void main(String[] args) throws SQLException {
		WorkersTableGateway gateway = new WorkersTableGateway();

		gateway.insert("W99", "Vorname", "Nachname", 1);
		ResultSet findWithLastName = gateway.findWithLastName("Nachname");
		printResultSet(findWithLastName);
		gateway.update("W99", "NewVorname", "Nachname", 1);

		ResultSet findAll = gateway.findAll();
		printResultSet(findAll);
		ResultSet findid = gateway.find("W99");
		printResultSet(findid);

		gateway.delete("W99");

	}

	private static void printResultSet(ResultSet resSet) throws SQLException {
		while (resSet.next()) {
			System.out.println(resSet.getString(1) + "|" + resSet.getString(2) + "|"
					+ resSet.getString(3) + "|" + resSet.getInt(4));
		}
	}

}
