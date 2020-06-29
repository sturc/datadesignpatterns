package de.dhbw.mosbach.dp.mapping.gateway.exercises;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import de.dhbw.mosbach.dp.MySQLUtil;
import de.dhbw.mosbach.dp.mapping.gateway.WorkersRowGateway;

public class WorkersFinder {



	public WorkersRowGateway find(String id) throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		String sql = "SELECT * FROM Workers WHERE id = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, id);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			return WorkersRowGateway.load(rs);
		} else {
			return null;
		}
	}

	public List<WorkersRowGateway> findWithLastName(String lastName) throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		String sql = "SELECT * FROM Workers WHERE lastname = ?";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setString(1, lastName);
		ResultSet rs = pstmt.executeQuery();
		List<WorkersRowGateway> result = new Vector<WorkersRowGateway>();
		while (rs.next()) {
			result.add(WorkersRowGateway.load(rs));
		}
		return result;
	}

	public List<WorkersRowGateway> findAll() {
		// TODO implement the method
		return new Vector<WorkersRowGateway>();
	}
}
