package de.dhbw.mosbach.dp.mapping.json;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonObjectMappingExample {

	private final static String WORKERS0FILE = "src/main/resources/workers0.json";

	public static void main(String[] args) {

		ObjectMapper mapper = new ObjectMapper();
		try {
			Workers worker = mapper.readValue(new File(WORKERS0FILE), Workers.class);
			// Convert object to JSON string and save into file directly
			worker.setFirstName("Test");
			mapper.writeValue(new File("src/main/resources/workerNew.json"), worker);

			// Convert object to JSON string
			String jsonInString = mapper.writeValueAsString(worker);
			System.out.println(jsonInString);

			// Convert object to JSON string and pretty print
			jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(worker);
			System.out.println(jsonInString);

		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
