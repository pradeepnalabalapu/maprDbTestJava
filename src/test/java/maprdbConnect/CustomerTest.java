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


public class CustomerTest {
	static String  JSON_FILE = "data/customer.json";
	String  TABLE_NAME ="Customer";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CustomerTable ct = new CustomerTable("maprdb_tables/Customer");
		try {
			ct.printTableInformation();
			System.out.println("\n\nTable contents before insert");
			System.out.println("================================");
			ct.printDocuments();
			System.out.println("================================");

			//Read data from a json file
			List<JSONObject> pl = getData(JSON_FILE);
			//Insert Json array into PersonTable
			//Inserting just 10 elements
			ct.insert(pl);
			
			System.out.println("\n\nTable contents after insert");
			System.out.println("===============================");
			ct.printDocuments();
			System.out.println("================================");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static ArrayList<JSONObject> getData(String json_file) {
		ArrayList<JSONObject> al = new ArrayList<JSONObject>();
		JSONParser jsonParser = new JSONParser();
		try (FileReader reader = new FileReader(json_file)) {
			Object obj = jsonParser.parse(reader);
			if (obj instanceof JSONObject){
				al.add((JSONObject)obj);
			} else if(obj instanceof JSONArray) {
				al = (ArrayList<JSONObject>)obj;
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return al;
	}


}






