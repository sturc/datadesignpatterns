package de.dhbw.mosbach.dp.mapping.datamapper;

public class Workers implements DomainObject {

	private String id;
	private String firstName;
	private String lastName;
	private int dep;

	public Workers(String id, String firstNameArg, String lastNameArg, int depArg) {
		this.id = id;
		this.firstName = firstNameArg;
		this.lastName = lastNameArg;
		this.dep = depArg;
	}

	@Override
	public String getId() {
		return id;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public int getDep() {
		return dep;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;

	}
	public String getFullName() {
		return firstName + " " + lastName;

	}

	public void checkSSNNumber() {
		// dummy method
	}

	@Override
	public String toString() {
		return id + " | " + firstName + " | " + lastName + " | " + dep;
	}

}
