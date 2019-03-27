package maprdbConnect;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONObject;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.JsonOptions;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;
import org.ojai.types.ODate;

import com.mapr.db.MapRDB;


public class HouseTable {
	
	private Connection connection;
	private DocumentStore store;
	
	/** create new table -- delete old one if it already exists  **/
	HouseTable(String tablePath) {
		connection = DriverManager.getConnection("ojai:mapr:");
		System.out.println("Successfully connected");
		if (connection.storeExists(tablePath)) {
			connection.deleteStore(tablePath);
		}
		store = connection.createStore(tablePath);
		System.out.println("GetStore successful");
	}
	
	public void insert(List<JSONObject> jl) {
		jl.forEach( j -> {
			//System.out.println("Creating document from "+j);
			final Document document = connection.newDocument(j);

		      System.out.println("\t inserting "+ document.getId());

		      // insert the OJAI Document into the DocumentStore
		      store.insertOrReplace(document);
		});
		
	}
	

	public void printTableInformation() throws IOException {
	    System.out.println("\n=============== TABLE INFO ===============");
	    System.out.println("==========================================\n");
	  }
	
	public void printDocuments() throws IOException {
		
		System.out.println("Printing all records");
		DocumentStream rs = store.find();
		for (final Document readRecord : rs) {
			System.out.println("\t"+readRecord.asJsonString(new JsonOptions().setPretty(true)));
			
		}
		rs.close();
	}
	
	public void printWhouseTransSums() throws IOException {
		System.out.println("Printing Whouse Trans Sums details");
		Query query = connection.newQuery()
				.select("sas_whouse_cm_item_brand_chan_trans_sum[].total_sales_amt_lifetime","sas_whouse_cm_item_brand_chan_trans_sum[].processed_dttm")
				.build();
		DocumentStream rs =store.find(query);
		for (final Document record: rs) {
			System.out.println("\t"+record.asJsonString(new JsonOptions().setPretty(true)));
		}
	}
	
	
}
;


