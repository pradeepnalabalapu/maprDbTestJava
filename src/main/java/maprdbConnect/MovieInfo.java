package maprdbConnect;


import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

public class MovieInfo {
	final Connection connection;
	MovieInfo() {
		Connection connection = DriverManager.getConnection("ojai:mapr://10.20.30.66:5678?auth=basic;user=mapr;password=mapr;ssl=true;sslCA=/opt/mapr/conf/ssl_truststore.pem;sslTargetNameOverride=psnode66.ps.lab;");
		//Connection connection = DriverManager.getConnection("ojai:mapr:");
		this.connection = connection;
		final DocumentStore store = connection.getStore("maprdb_tables/movies");

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

}

