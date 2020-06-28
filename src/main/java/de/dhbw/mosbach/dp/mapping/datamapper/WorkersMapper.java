package de.dhbw.mosbach.dp.mapping.datamapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import de.dhbw.mosbach.dp.MySQLUtil;

public class WorkersMapper extends AbstractMapper {

	protected String findStatement() {
		return "SELECT " + COLUMNS + "  FROM Workers	" + "  WHERE ID = ?";
	}

	public static final String COLUMNS = " ID, FirstName, LastName, Dep ";

	public Workers find(String id) {
		return (Workers) abstractFind(id);
	}

	private static String findLastNameStatement = "SELECT " + COLUMNS + "  FROM Workers "
			+ "  WHERE UPPER(LastName) like UPPER(?)" + "  ORDER BY LastName";

	public List<Workers> findByLastName(String name) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = MySQLUtil.getConnection();
			stmt = conn.prepareStatement(findLastNameStatement);
			stmt.setString(1, name);
			rs = stmt.executeQuery();
			List<DomainObject> loadedDO = loadAll(rs);
			List<Workers> result = new ArrayList<Workers>();
			for (DomainObject doOb : loadedDO) {
				result.add((Workers) doOb);
			}
			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			MySQLUtil.free2(rs, stmt, conn);
		}
	}

	private static final String updateStatementString = "UPDATE Workers "
			+ "  SET FirstName = ?, LastName = ?, Dep = ? " + "  WHERE ID = ?";

	public void update(Workers subject) throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		PreparedStatement updateStatement = null;

		try {
			updateStatement = conn.prepareStatement(updateStatementString);
			updateStatement.setString(1, subject.getFirstName());
			updateStatement.setString(2, subject.getLastName());
			updateStatement.setInt(3, subject.getDep());
			updateStatement.setString(4, subject.getId());
			updateStatement.execute();
			removeWorkers(subject.getId());
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			MySQLUtil.free(updateStatement, conn);
		}
	}

	@Override
	protected DomainObject doLoad(String id, ResultSet rs) throws SQLException {
		String firstNameArg = rs.getString(2);
		String lastNameArg = rs.getString(3);
		int depArg = rs.getInt(4);
		return new Workers(id, firstNameArg, lastNameArg, depArg);
	}


	protected String insertStatement() {
		return "INSERT INTO Workers VALUES (?, ?, ?, ?)";
	}

	protected void doInsert(DomainObject abstractSubject, PreparedStatement stmt) throws SQLException {
		Workers subject = (Workers) abstractSubject;
		stmt.setString(2, subject.getFirstName());
		stmt.setString(3, subject.getLastName());
		stmt.setInt(4, subject.getDep());
	}

	public void delete(Workers subject) throws SQLException {
		Connection conn = MySQLUtil.getConnection();
		PreparedStatement deleteStatement = null;
		try {
			String sql = "DELETE FROM Workers WHERE ID = ?";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, subject.getId());
			pstmt.executeUpdate();
			removeWorkers(subject.getId());
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			MySQLUtil.free(deleteStatement, conn);
		}
	}

}
