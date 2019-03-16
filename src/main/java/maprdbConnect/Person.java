package maprdbConnect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ojai.types.ODate;

import java.util.ArrayList;
import java.util.List;



import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;
import org.ojai.types.ODate;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Person {
	String id;
	String first;
	String last;
	ODate dob;
	static SimpleDateFormat pft = new SimpleDateFormat("M/d/yyyy");
	static SimpleDateFormat sft = new SimpleDateFormat("M/d/yyyy");
	
	public Person() {
	}
	
	@JsonProperty("_id")
	  public String getId() {
	    return id;
	  }

	  public void setId(String id) {
	    this.id = id;
	  }
	
	@JsonProperty("first_name")
	public String getFirstName() {
		return first;
	}
	
	public void setFirstName(String first) {
		this.first = first;
	}
	

	@JsonProperty("last_name")
	public String getLastName() {
		return last;
	}
	

	public void setLastName(String last) {
		this.last = last;
	}
	
	@JsonProperty("DOB")
	public ODate getDob() {
		return dob;
	}
	
	public void setDob(ODate dob) {
		this.dob = dob;
	}
	
	
	
	Person(JSONObject p) {
			first = (String) p.get("first");
			last =  (String) p.get("last");
			String dobStr = (String) p.get("dob");
			
			try {
				Date dob_java = pft.parse(dobStr); 
				dob = new ODate(dob_java);
			
	
			} catch (java.text.ParseException e) {
				e.printStackTrace();
			}
			id = first+last;
	}

	public void print() {
		System.out.printf("first = %s last =%s dob = %s\n",first,last,sft.format(dob.toDate()));
	}
	
	@Override
	public String toString() {
		return "Person{" +
			"FirstName='" + first + '\''+
			", LastName='" + last +'\'' +
			", DOB=" + dob +
			"}";
	}
}
