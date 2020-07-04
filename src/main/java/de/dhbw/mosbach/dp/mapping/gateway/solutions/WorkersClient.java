package de.dhbw.mosbach.dp.mapping.gateway.solutions;

import java.sql.SQLException;
import java.util.List;

import de.dhbw.mosbach.dp.mapping.gateway.WorkersDomainObject;
import de.dhbw.mosbach.dp.mapping.gateway.WorkersRowGateway;

public class WorkersClient {

	public static void main(String[] args) throws SQLException {

		// work with finder
		WorkersFinder wf = new WorkersFinder();
		System.out.println(wf.find("W1"));
		WorkersRowGateway w99 = new WorkersRowGateway("W99", "Vorname", "Nachname", 1);
		w99.insert();
		System.out.println(wf.findWithLastName("Nachname"));
		w99.setLastName("NachnameTest");

		System.out.println(wf.find("W99"));
		w99.delete();
		System.out.println(wf.find("W99"));
		List<WorkersRowGateway> findAllResult = wf.findAll();
		for (WorkersRowGateway currWorker : findAllResult) {
			System.out.println(currWorker);
		}
		// work with domain objects

		WorkersDomainObject wdo99 = new WorkersDomainObject(wf.find("W1"));
		System.out.println(wdo99.getFullName());

	}

}
