package de.dhbw.mosbach.dp.mapping.gateway;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.dhbw.mosbach.dp.MySQLUtil;

public class WorkersRowGateway {


	private String id;
	private String firstName;
	private String lastName;
	private int dep;


	public WorkersRowGateway(String idArg, String firstNameArg, String lastNameArg, int department) {
		this.id = idArg;
		this.firstName = firstNameArg;
		this.lastName = lastNameArg;
		this.dep = department;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public int getDep() {
		return dep;
	}

	public void setDep(int dep) {
		this.dep = dep;
	}

	public void update() throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		String sql = "UPDATE Workers SET LastName = ?, FirstName = ?, Dep = ? WHERE ID = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, firstName);
		pstmt.setString(2, lastName);
		pstmt.setInt(3, dep);
		pstmt.setString(4, id);
		pstmt.executeUpdate();
	}

	public int insert() throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		String sql = "insert into Workers (ID,FirstName,LastName,Dep) values(?,?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		pstmt.setString(2, firstName);
		pstmt.setString(3, lastName);
		pstmt.setInt(4, dep);
		return pstmt.executeUpdate();
	}

	public void delete() throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		String sql = "DELETE  FROM Workers WHERE id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		pstmt.executeUpdate();
	}

	public static WorkersRowGateway load(ResultSet rs) throws SQLException {
		String idArg = rs.getString(1);
		String firstNameArg = rs.getString(2);
		String lastNameArg = rs.getString(3);
		int department = rs.getInt(4);
		return new WorkersRowGateway(idArg, firstNameArg, lastNameArg, department);
	}

	@Override
	public String toString() {
		return id + " | " + firstName + " | " + lastName + " | " + dep;
	}

}
