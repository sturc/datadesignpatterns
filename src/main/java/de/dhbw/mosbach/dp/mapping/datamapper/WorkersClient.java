package de.dhbw.mosbach.dp.mapping.datamapper;

import java.sql.SQLException;
import java.util.List;

public class WorkersClient {

	public static void main(String[] args) throws SQLException {

		WorkersMapper wm = new WorkersMapper();
		System.out.println(wm.find("W1"));
		List<Workers> findByLastName = wm.findByLastName("Brown");
		for (Workers currWorkers : findByLastName) {
			System.out.println(currWorkers);
		}
		Workers w99 = new Workers("W99", "Vorname", "Nachname", 1);
		wm.insert(w99);
		System.out.println(wm.findByLastName("Nachname"));
		Workers w99db = wm.find("W99");
		System.out.println(w99db == wm.find("W99"));
		w99.setLastName("NachName2");
		wm.update(w99);
		System.out.println(wm.find("W99"));
		wm.delete(w99);
		System.out.println(wm.find("W99"));
	}

}
