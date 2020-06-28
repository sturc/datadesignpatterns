package de.dhbw.mosbach.dp.mapping.gateway;

public class WorkersDomainObject {

	private WorkersRowGateway data;

	public WorkersDomainObject(WorkersRowGateway data) {
		this.data = data;
	}

	public String getLastName() {
		return data.getLastName();
	}

	public String getFullName() {
		return data.getFirstName() + " " + data.getLastName();

	}

}
