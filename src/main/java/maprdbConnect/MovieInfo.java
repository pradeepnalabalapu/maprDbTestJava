package maprdbConnect;


import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;

public class MovieInfo {
	private static Connection connection;
	MovieInfo() {
		//Connection connection = DriverManager.getConnection("ojai:mapr:10.20.30.66:5678?auth=basic;user=mapr;password=mapr;ssl=true;sslCA=/opt/mapr/conf/ssl_truststore.pem;sslTargetNameOverride=psnode66.ps.lab;");
		
		Connection connection = DriverManager.getConnection("ojai:mapr:");
		System.out.println("Successfully connected");
		
		
		this.connection = connection;
		final DocumentStore store = connection.getStore("/user/mapr/maprdb_tables/movies");
		System.out.println("GetStore successful");

	    // fetch all OJAI Documents from this store
	    final DocumentStream stream = store.find();

	    for (final Document userDocument : stream) {
	      // Print the OJAI Document
	      System.out.println(userDocument.asJsonString());
	    }

	    // Close this instance of OJAI DocumentStore
	    store.close();

	    // close the OJAI connection and release any resources held by the connection
	    connection.close();

	    System.out.println("==== End Application ===");
	    
	}
	Table table;
	private Table getTable(String tableName) throws DBException {
	    
		if (MapRDB.tableExists(tableName)) {
			table = MapRDB.getTable(tableName);
		}
		return table;
	}
	
	public void printTableInformation(String tableName) throws IOException {
	    Table table = MapRDB.getTable(tableName);
	    System.out.println("\n=============== TABLE INFO ===============");
	    System.out.println(" Table Name : " + table.getName());
	    System.out.println(" Table Path : " + table.getPath());
	    System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));
	    System.out.println("==========================================\n");
	  }
	
	public void printDocuments(String tableName) throws IOException {
		Table table = MapRDB.getTable(tableName);
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


		