package de.dhbw.mosbach.dp.mapping.json.exercises;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ProjectsJsonObjectMappingTest {

	private final static String POJECTSFILE = "src/main/resources/projects-normalized.json";

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();
		try {
			Projects projects = mapper.readValue(new File(POJECTSFILE), Projects.class);
			// Convert object to JSON string and save into file directly
			// TODO add implementation

			// Convert object to JSON string
			// TODO add implementation

			// Convert object to JSON string and pretty print
			// TODO add implemenation

		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
