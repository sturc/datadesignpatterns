package de.dhbw.mosbach.dp.mapping.datamapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import de.dhbw.mosbach.dp.MySQLUtil;


public abstract class AbstractMapper {


	abstract protected String findStatement();


	protected DomainObject abstractFind(String id) {
		DomainObject result = (DomainObject) domainObjectCache.get(id);
		if (result != null)
			return result;
		PreparedStatement findStatement = null;
		Connection conn = null;
		try {
			conn = MySQLUtil.getConnection();
			findStatement = conn.prepareStatement(findStatement());
			findStatement.setString(1, id);
			ResultSet rs = findStatement.executeQuery();
			if (rs.next()) {
				result = load(rs);
				return result;
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			MySQLUtil.free(findStatement, conn);
		}
	}

	protected DomainObject load(ResultSet rs) throws SQLException {
		String id = rs.getString(1);
		if (domainObjectCache.containsKey(id))
			return (DomainObject) domainObjectCache.get(id);
		DomainObject result = doLoad(id, rs);
		domainObjectCache.put(id, result);
		return result;
	}

	abstract protected DomainObject doLoad(String id, ResultSet rs) throws SQLException;

	// important: This is only save for multithreading within one JVM
	private static HashMap<String, DomainObject> domainObjectCache = new HashMap<String, DomainObject>();

	public static void addWorker(DomainObject arg) {
		AbstractMapper.domainObjectCache.put(arg.getId(), arg);
	}

	public static DomainObject getWorkers(String id) {
		return AbstractMapper.domainObjectCache.get(id);
	}

	public static DomainObject removeWorkers(String id) {
		return AbstractMapper.domainObjectCache.remove(id);
	}

	protected List<DomainObject> loadAll(ResultSet rs) throws SQLException {
		List<DomainObject> result = new ArrayList<DomainObject>();
		while (rs.next())
			result.add(load(rs));
		return result;
	}

	public String insert(DomainObject subject) {
		PreparedStatement insertStatement = null;
		Connection conn = null;
		try {
			conn = MySQLUtil.getConnection();
			insertStatement = conn.prepareStatement(insertStatement());
			insertStatement.setString(1, subject.getId());
			doInsert(subject, insertStatement);
			insertStatement.execute();
			domainObjectCache.put(subject.getId(), subject);
			return subject.getId();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			MySQLUtil.free(insertStatement, conn);
		}
	}

	abstract protected String insertStatement();

	abstract protected void doInsert(DomainObject subject, PreparedStatement insertStatement) throws SQLException;

}
