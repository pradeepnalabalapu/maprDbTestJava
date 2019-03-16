package maprdbConnect;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;
import org.ojai.types.ODate;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;

public class PersonTable {
	
	private Table table;
	
	/** create new table -- delete old one if it already exists  **/
	PersonTable(String tablePath) {
		if  (MapRDB.tableExists(tablePath)) {
			MapRDB.deleteTable(tablePath);
		} 
		table = MapRDB.createTable(tablePath);
		
	}
	
	public void insert(List<Person> pl) {
		pl.forEach( p -> {
			//System.out.println("Creating document from "+p);
			Document document = MapRDB.newDocument(p);
			table.insertOrReplace(document);
		});
		table.flush();
	}
	
	public void set30YearBirthday() {
		//First query and get the id's and dob
		DocumentStream rs = table.find();
		Iterator<Document> itrs = rs.iterator();
		Document readRecord;
		while (itrs.hasNext()) {
			readRecord = itrs.next();
			//System.out.println("\t" + readRecord);
			String id = readRecord.getIdString();
			ODate dob = readRecord.getDate("DOB");
			//System.out.printf("Got id=%s DOB="+dob+"\n",id);
			Date dob30 = new Date(dob.toDate().getTime() + 30*365*24*60*60*1000L);
			DocumentMutation mutation = MapRDB.newMutation()
					.set("DOB30", new ODate(dob30) );
			table.update(id, mutation);
		}
		

		/*
		DocumentMutation mutation = MapRDB.newMutation()
				.set("audit_date", auditDate);
		table.checkAndUpdate(_id, condition, mutation)
		*/
		
	}
	
	
	public void printTableInformation() throws IOException {
	    System.out.println("\n=============== TABLE INFO ===============");
	    System.out.println(" Table Name : " + table.getName());
	    System.out.println(" Table Path : " + table.getPath());
	    System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));
	    System.out.println("==========================================\n");
	  }
	
	public void printDocuments() throws IOException {
		
		System.out.println("Printing all records");
		DocumentStream rs = table.find();
		Iterator<Document> itrs = rs.iterator();
		Document readRecord;
		while (itrs.hasNext()) {
			readRecord = itrs.next();
			System.out.println("\t" + readRecord);
		}
		rs.close();
	}

}
;
