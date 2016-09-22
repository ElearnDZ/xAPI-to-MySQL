package server;
import gov.adlnet.xapi.model.*;
import gov.adlnet.xapi.client.*;
import gov.adlnet.xapi.model.adapters.*;
import gov.adlnet.xapi.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class mysql {
	private static Map<String, String> nameMap;
	 // stores nameMap from statements to MySQL
	private static Map<String, String> system_variables;
	// stores MySQL's system table's variables
	private static Map<String, String> unit_variables;
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//getConnection();
		//post();
		mapSetup();

		StatementClient client1 = new StatementClient("https://lrs.adlnet.gov/xapi/","xapi-tools","xapi-tools");
		double diff = 0.0;
		while (true) {
			diff = 0;
			Thread.sleep(8000);
			List<Statement> newStatements;
			try {
				newStatements = client1.getStatements("/statements?limit=50").getStatements();
			}
			catch (Exception e) {
				continue;
			}
			if (newStatements == null || newStatements.isEmpty()) {
				continue;
			}
		//&verb=http://74.208.144.213:8000/verbs/check  since=2016-08-08T13:34:56.266205+00:00
			Set<String> set= new HashSet<String>();
			// hashset to make sure each round only read one entry for each variable
			for (int i = 0;i < 40; i++) {
				//here we only care about the first 40 entries
				Statement newStatement = newStatements.get(i);
				//String time = newStatement.getTimestamp();
				
				if (newStatement.getActor().toString().equals("ICS System")) {
					// this is PLC Statement
					String xAPIname = getName(newStatement);
					//System.out.println(xAPIname);
					String MySQLname = nameMap.get(xAPIname);
					if (MySQLname == null) {
						continue;
					}
					if (!set.add(MySQLname)) {
						continue;
					}
					Map<String, JsonElement> map = newStatement.getContext().getExtensions();
					if (map == null || map.isEmpty()) {
						continue;
					}
					// Need two JsonElement since the key can be in two forms
					JsonElement js2 = map.get("http://adlnet.gov/expapi/extensions/checklist");
					JsonElement js1 = map.get("http://74.208.144.213:8000/extensions/checklist");
					
					String s = js1 != null ? js1.toString() : js2.toString();
					int[] matchIndex = strstr(s, "checkboxoption");
					String value = s.substring(matchIndex[0], matchIndex[1]);
					if (value != null) {
						double val = 0.0;
						try
						{
						  val = Double.parseDouble(value);
						}
						catch(NumberFormatException e)
						{
						  //not a double
							continue;
						}
						if (system_variables.containsKey(MySQLname)) {
							diff += Math.abs(val - Double.parseDouble(system_variables.get(MySQLname)));
							system_variables.put(MySQLname, value);
						} else if (unit_variables.containsKey(MySQLname)) {
							diff += Math.abs(val - Double.parseDouble(unit_variables.get(MySQLname)));
							unit_variables.put(MySQLname, value);
						}
					}
				}
			}
			if (diff > 0.1) {
				//only post if variables change
				postSystem();
				postUnit();
			}
		}
		
		
	}
	private static void mapSetup() {
		system_variables = new HashMap<String, String>();
		system_variables.put("ozone_leak", "0");
		system_variables.put("oxygen_leak", "0");
		system_variables.put("air_temperature", "0");
		system_variables.put("water_circulation", "0");
		system_variables.put("water", "0");
		system_variables.put("water_rate", "0");
		system_variables.put("first_generator_ozone_production", "0");
		system_variables.put("second_generator_ozone_production", "0");
		system_variables.put("third_generator_ozone_production", "0");
		system_variables.put("overall_ozone_production", "0");
		
		unit_variables = new HashMap<String, String>();
		unit_variables.put("output_air_temperature", "0");
		unit_variables.put("output_water_temperature", "0");
		unit_variables.put("pressure", "0");
		unit_variables.put("ozone_volume", "0");
		unit_variables.put("ozone_density", "0");
		unit_variables.put("local", "0");
		unit_variables.put("failure", "0");
		unit_variables.put("running", "0");
		unit_variables.put("emergency_stop", "0");
		
		nameMap = new HashMap<String, String>();
		nameMap.put("Ozone_leak :=", "ozone_leak");
		nameMap.put("Oxygen leak :=", "oxygen_leak");
		nameMap.put("Air temperature :=", "air_temperature");
		nameMap.put("Water circulatoin :=", "water_circulation");
		nameMap.put("Water (front injection) :=", "water");
		nameMap.put("Water rate (front injection) :=", "water_rate");
		nameMap.put("#1 generator ozone production :=", "first_generator_ozone_production");
		nameMap.put("#2 generator ozone production :=", "second_generator_ozone_production");
		nameMap.put("#3 generator ozone production :=", "third_generator_ozone_production");
		nameMap.put("Overall ozone production :=", "overall_ozone_production");
		
		nameMap.put("Output air temperature :=", "output_air_temperature");
		nameMap.put("Output water temperature :=", "output_water_temperature");
		nameMap.put("Pressure :=", "pressure");
		nameMap.put("Ozone vulume :=", "ozone_volume");
		nameMap.put("Oone density :=", "ozone_density");
		nameMap.put("Local :=", "local");
		nameMap.put("Failure :=", "failure");
		nameMap.put("Running :=", "running");
		nameMap.put("Emergency stop :=", "emergency_stop");
	}
	
	private static String getName(Statement s) {
		if (s == null) {
			return null;
		}
		IStatementObject ob = s.getObject();
		return ob.toString();
	}
	
	 private static int[] strstr(String large, String small) {
		 	int[] result = new int[2];
		    if (small.length() == 0)
		    {
		      return result;
		    } 
		    for (int i = 0; i <= large.length() - small.length(); i++)
		    {
		      if(equals(large, i, small))
		      {
		        result[0] = i + 17;
		        i = i + 17;
		        while (i < large.length() && large.charAt(i) != '"') {
		        	i++;
		        }
		        result[1] = i;
		      }
		    }
		    return result;
		  }
	private static boolean equals(String large, int start, String small)
		  {
		    for (int i = 0; i < small.length(); i++)
		    {
		      if (large.charAt(start + i) != small.charAt(i))
		      {
		        return false;
		      }
		    }
		    return true;
		  }
	
	public static Connection getConnection() throws Exception {
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://74.208.144.213:3306/scada";//"jdbc:mysql://localhost:3306/scada";
			String username = "regular01";
			String password = "regular01";
			Class.forName(driver);
			
			Connection conn = DriverManager.getConnection(url, username, password);
			System.out.println("connected");
			return conn;
		} catch(Exception e) {
			System.out.println(e);
		}
		return null;
	}
	public static void postSystem() throws Exception {
		try {
			Connection con = getConnection();
			String query = "INSERT INTO system_table (ozone_leak, oxygen_leak, air_temperature, water_circulation, water, water_rate, "
					+ "first_generator_ozone_production, second_generator_ozone_production, third_generator_ozone_production, "
					+ "overall_ozone_production) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement preparedStmt = con.prepareStatement(query);
		    preparedStmt.setString (1, system_variables.get("ozone_leak"));
		    preparedStmt.setString (2, system_variables.get("oxygen_leak"));
		    preparedStmt.setString (3, system_variables.get("air_temperature"));
		    preparedStmt.setString (4, system_variables.get("water_circulation"));
		    preparedStmt.setString (5, system_variables.get("water"));
		    preparedStmt.setString (6, system_variables.get("water_rate"));
		    preparedStmt.setString (7, system_variables.get("first_generator_ozone_production"));
		    preparedStmt.setString (8, system_variables.get("second_generator_ozone_production"));
		    preparedStmt.setString (9, system_variables.get("third_generator_ozone_production"));
		    preparedStmt.setString (10, system_variables.get("overall_ozone_production"));
		    preparedStmt.execute();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	public static void postUnit() throws Exception {
		try {
			Connection con = getConnection();
			String query = "INSERT INTO unit_table (output_air_temperature, output_water_temperature, pressure, ozone_volume, "
					+ "ozone_density, local, failure, running, emergency_stop) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement preparedStmt = con.prepareStatement(query);
		    preparedStmt.setString (1, unit_variables.get("output_air_temperature"));
		    preparedStmt.setString (2, unit_variables.get("output_water_temperature"));
		    preparedStmt.setString (3, unit_variables.get("pressure"));
		    preparedStmt.setString (4, unit_variables.get("ozone_volume"));
		    preparedStmt.setString (5, unit_variables.get("ozone_density"));
		    preparedStmt.setString (6, unit_variables.get("local"));
		    preparedStmt.setString (7, unit_variables.get("failure"));
		    preparedStmt.setString (8, unit_variables.get("running"));
		    preparedStmt.setString (9, unit_variables.get("emergency_stop"));
		    preparedStmt.execute();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
