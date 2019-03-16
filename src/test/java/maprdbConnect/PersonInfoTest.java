package maprdbConnect;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.ojai.types.ODate;

import com.google.gson.JsonParseException;


public class PersonInfoTest {
	static String  JSON_FILE = "data/person_info.json";
	String  TABLE_NAME ="personInfo";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PersonTable pt = new PersonTable("maprdb_tables/person_info");
		try {
			pt.printTableInformation();
			System.out.println("\n\nTable contents before insert");
			System.out.println("================================");
			pt.printDocuments();
			System.out.println("================================");

			//Read data from a json file
			List<Person> pl = getData(JSON_FILE);
			//Insert Json array into PersonTable
			//Inserting just 10 elements
			pt.insert(pl.subList(0, 9));
			System.out.println("\n\nTable contents after insert");
			System.out.println("===============================");
			pt.printDocuments();
			System.out.println("================================");

			//update documents to add a new field DOB30 = DOB + 30 years approx.
			pt.set30YearBirthday();
			System.out.println("\n\nTable contents after mutation");
			System.out.println("================================");
			pt.printDocuments();
			System.out.println("================================");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static ArrayList<Person> getData(String json_file) {
		ArrayList<Person> al = new ArrayList<Person>();
		JSONParser jsonParser = new JSONParser();

		try (FileReader reader = new FileReader(json_file)) {
			Object obj = jsonParser.parse(reader);
			JSONArray personList = (JSONArray) obj;
			//System.out.println(personList);
			personList.forEach( pers -> al.add(new Person((JSONObject)pers)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return al;
	}

	private static void parsePerson(JSONObject p) {
		new Person(p).print();
	}

}




