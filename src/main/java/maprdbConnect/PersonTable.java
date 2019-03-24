package maprdbConnect;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.types.ODate;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;

public class PersonTable {
	
	private Connection connection;
	private DocumentStore store;
	
	/** create new table -- delete old one if it already exists  **/
	PersonTable(String tablePath) {
		connection = DriverManager.getConnection("ojai:mapr:");
		System.out.println("Successfully connected");
		if (connection.storeExists(tablePath)) {
			connection.deleteStore(tablePath);
		}
		store = connection.createStore(tablePath);
		System.out.println("GetStore successful");
	}
	
	public void insert(List<Person> pl) {
		pl.forEach( p -> {
			//System.out.println("Creating document from "+p);
			final Document document = connection.newDocument(p);

		      System.out.println("\t inserting "+ document.getId());

		      // insert the OJAI Document into the DocumentStore
		      store.insertOrReplace(document);
		});
		
	}
	
	public void set30YearBirthday() {
		//First query and get the id's and dob
		final DocumentStream rs = store.find();

		for (final Document readRecord : rs) {		
			//System.out.println("\t" + readRecord);
			String id = readRecord.getIdString();
			ODate dob = readRecord.getDate("DOB");
			//System.out.printf("Got id=%s DOB="+dob+"\n",id);
			Date dob30 = new Date(dob.toDate().getTime() + 30*365*24*60*60*1000L);
			DocumentMutation mutation = connection.newMutation()
					.set("DOB30", new ODate(dob30) );
			store.update(id, mutation);
		}
	
		/*
		DocumentMutation mutation = MapRDB.newMutation()
				.set("audit_date", auditDate);
		table.checkAndUpdate(_id, condition, mutation)
		*/
		
	}
	
	
	public void printTableInformation() throws IOException {
	    System.out.println("\n=============== TABLE INFO ===============");
	    /*
	    System.out.println(" Table Name : " + store.toString();
	    System.out.println(" Table Path : " + store.getPath);
	    System.out.println(" Table Infos : " + Arrays.toString(store.getTabletInfos()));
	    */
	    System.out.println("==========================================\n");
	  }
	
	public void printDocuments() throws IOException {
		
		System.out.println("Printing all records");
		DocumentStream rs = store.find();
		for (final Document readRecord : rs) {
			System.out.println("\t" + readRecord);
		}
		rs.close();
	}

}
;

	    