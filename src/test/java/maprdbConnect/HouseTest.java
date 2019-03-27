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


public class HouseTest {
	static String  JSON_FILE = "data/house.json";
	String  TABLE_NAME ="House";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HouseTable ht = new HouseTable("maprdb_tables/House");
		try {
			ht.printTableInformation();
			System.out.println("\n\nTable contents before insert");
			System.out.println("================================");
			ht.printDocuments();
			System.out.println("================================");

			//Read data from a json file
			List<JSONObject> pl = getDataFromFile(JSON_FILE);
			//Insert Json array into PersonTable
			//Inserting just 10 elements
			ht.insert(pl);
			
			System.out.println("\n\nTable contents after insert");
			System.out.println("===============================");
			ht.printDocuments();
			System.out.println("================================");

			//Testing query of level 2 data
			ht.printWhouseTransSums();			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static ArrayList<JSONObject> getDataFromFile(String json_file) {
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







