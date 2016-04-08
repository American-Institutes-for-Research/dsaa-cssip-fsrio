package com.jbt;

import java.io.*;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.text.WordUtils;


public class NIH {
	
	HashMap<String, String> abstracts = new HashMap<String, String>();
	HashMap<String, String> indexMap = new HashMap<String, String>();
	
	public static String nihMain(String inputfolder, String inputfolder_abstracts, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		
		NIH obj  =  new NIH();
		Connection conn = MysqlConnect.connection(host,user,passwd);
		obj.abstracts(inputfolder_abstracts);
		obj.projects(outfolder,inputfolder,conn,dbname);
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}		

		return "NIH";
	
	}

	public File[] getFiles(String dir) {
		File[] files = new File(dir).listFiles();

		return files;
	}

	public void abstracts(String inputfolder_abstracts) throws IOException {
		File[] files = getFiles(inputfolder_abstracts);
		for (File file : files) {

			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(new FileReader(file));
			try {	
			for (CSVRecord record : records) {
					try {
					
					abstracts.put(record.get("APPLICATION_ID"), record.get("ABSTRACT_TEXT"));
					}
					catch (Exception e) {
						
					}
				}
			} catch (Exception ee) {
				
			}
			
		}
	}

	public void projects(String outfolder, String inputfolder,Connection conn, String dbname) throws IOException {
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVPrinter csvout = new CSVPrinter(new FileWriter(outfolder+"NIH_"+currentStamp+".csv"), CSVFormat.EXCEL.withDelimiter('\t'));
		
		String[] header = {"project__PROJECT_NUMBER","project__AGENCY_FULL_NAME", "agency_index__aid", 
				"project__PROJECT_OBJECTIVE", "project__PROJECT_TITLE",
				"project__PROJECT_START_DATE", "project__PROJECT_END_DATE",
				"investigator_data__name",
				"institution_data__INSTITUTION_NAME", "institution_data__INSTITUTION_CITY",
				"states__states_abbrv","institution_data__INSTITUTION_STATE", "institution_data__INSTITUTION_COUNTRY",
				"institution_index__inst_id", 
				"investigator_index__inv_id"};
		
		csvout.printRecord(header);

		File[] files = getFiles(inputfolder);
		for (File file : files) {
			if (!file.getName().contains(".csv")) {
				continue;
			}
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(new FileReader(file));
			for (CSVRecord record : records) {
				String project__AGENCY_FULL_NAME = "";
				String project__PROJECT_OBJECTIVE= "";
				String countries__COUNTRY_NAME = "";
				String project__PROJECT_TITLE = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project__PROJECT_FUNDING = "";
				String project__PROJECT_TYPE = "3";
				String project__LAST_UPDATE = "";
				String project__ACTIVITY_STATUS = "0";
				String project__DATE_ENTERED = "";
				String investigator_data__name = "";
				String project__PROJECT_NUMBER = "";
				String institution_data__INSTITUTION_NAME = "";
				String institution_data__city_name = "";
				String states__states_abbrv = "";
				String comment = "";
				int institution_index__inst_id = -1; 
				int institution_data__INSTITUTION_STATE = -1; 
				int investigator_index__inv_id = -1;
				int agency_index__aid = -1;
				
				//Check if it is food safety at all before proceeding
				String foodFlag = record.get("NIH_SPENDING_CATS");
				if (!foodFlag.toLowerCase().contains("food safety")) {
					continue;
				}
				
				// Project information
				project__AGENCY_FULL_NAME = WordUtils.capitalize(record.get("IC_NAME").toLowerCase())
						.replace("Of","of").replace("And", "and")
						.replace("&", "and").replace("Eunice Kennedy Shriver ", "").replace("Lung, and","Lung and");
				project__PROJECT_OBJECTIVE = abstracts.get(record.get("APPLICATION_ID"))
						.replace("DESCRIPTION (provided by applicant): ","")
						.replace("Description (provided by the applicant): ", "")
						.replace("Seeinstructions): ", "")
						.replace("DESCRIPTION (provided by applicant)","").trim();
				project__PROJECT_TITLE = WordUtils.capitalize(record.get("PROJECT_TITLE"),' ','-');
				project__PROJECT_FUNDING = record.get("TOTAL_COST");
				project__PROJECT_START_DATE = record.get("PROJECT_START");
				project__PROJECT_END_DATE = record.get("PROJECT_END");
				project__PROJECT_NUMBER = record.get("FULL_PROJECT_NUM");
				
				Date dateNow = new Date();
				long days = 0;
				try {
					Date dateProj = new Date(project__PROJECT_END_DATE);

					long diff = dateNow.getTime() - dateProj.getTime();
					days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
				}
				catch (Exception e) {;}
				
				if (days >= 0)
					project__ACTIVITY_STATUS = "1";

				else
					project__ACTIVITY_STATUS = "4";
				project__LAST_UPDATE = dateFormat.format(current);
				DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
				project__DATE_ENTERED = dateFormatEnter.format(current);
				try {
					project__PROJECT_START_DATE = project__PROJECT_START_DATE.substring(project__PROJECT_START_DATE.length()-4);
				}
				catch(Exception e ) {;}
				
				try {
					project__PROJECT_END_DATE = project__PROJECT_END_DATE.substring(project__PROJECT_END_DATE.length()-4);	
				}
				catch (Exception e) {;}
				// Get Institution information
				institution_data__INSTITUTION_NAME = WordUtils.capitalize(record.get("ORG_NAME").toLowerCase(),' ','-','/')
						.replace("Of","of").replace("And", "and");
				institution_data__city_name = WordUtils.capitalize(record.get("ORG_CITY").toLowerCase(),' ','-');
				states__states_abbrv = record.get("ORG_STATE");
				countries__COUNTRY_NAME = WordUtils.capitalize(record.get("ORG_COUNTRY").toLowerCase(),' ','-');
				
				// See if the institution ID exists, if not make it -1 to reflect we need to add.
				String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME REGEXP ?;";
				ResultSet rs = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
					preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
					rs = preparedStmt.executeQuery();
					rs.next();
					institution_index__inst_id = Integer.parseInt(rs.getString(1));
				}
				catch (Exception e) {
					comment = "Please populate institution fields by exploring the institution named on the project.";
				}


				// See if the country ID exists, if not make it -1 to reflect we need to add.
				String institution_data__INSTITUTION_COUNTRY = "-1"; 
				String GetcountryIDsql = "SELECT ID FROM  "+dbname+"countries WHERE COUNTRY_NAME = ?;";
				ResultSet rs2 = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetcountryIDsql);
					preparedStmt.setString(1, countries__COUNTRY_NAME);
					rs2 = preparedStmt.executeQuery();
					rs2.next();
					institution_data__INSTITUTION_COUNTRY = rs2.getString(1);
				}
				catch (Exception e) {
					institution_data__INSTITUTION_COUNTRY = countries__COUNTRY_NAME;
				}


				// See if the state ID exists, if not make it -1 to reflect we need to add.
				String GetstateIDsql = "SELECT ID FROM  "+dbname+"states WHERE abbrv = ?;";
				ResultSet rs3 = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetstateIDsql);
					preparedStmt.setString(1, states__states_abbrv);
					rs3 = preparedStmt.executeQuery();
					rs3.next();
					institution_data__INSTITUTION_STATE = Integer.parseInt(rs3.getString(1));
				}
				catch (Exception e) {
					;
				}

				// Get Investigator information
				try {
					investigator_data__name = record.get("PI_NAMEs");
				}
				catch(Exception e) {
					try {
						investigator_data__name = record.get("PI_NAMES");
					}
					catch(Exception e2) {
						try {
							investigator_data__name = record.get("PI_NAME");

						}
						catch (Exception e3)
						{
							;
						}
					}
				}
				investigator_data__name = investigator_data__name.replace("(contact)", "");
				
				String[] names = investigator_data__name.split(";");
				for (String s :names) {
					investigator_data__name = WordUtils.capitalizeFully(s,' ','-','\'');
					String piLastName = investigator_data__name.split(", ")[0];
					String piFirstName = investigator_data__name.split(", ")[1];

					// Let us see if we can find the investigator in the already existing data. 

					String GetInvestigatorSQL = "SELECT ID FROM  "+dbname+"investigator_data WHERE NAME LIKE ?;";
					ResultSet rs6 = null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
						preparedStmt.setString(1, investigator_data__name);
						rs6 = preparedStmt.executeQuery();
						rs6.next();
						investigator_index__inv_id = Integer.parseInt(rs6.getString(1));
					}
					catch (Exception e) {
						String query = "SELECT * FROM  "+dbname+"investigator_data where name regexp ^?;";
						ResultSet result = null;
						try {
							PreparedStatement preparedStmt = conn.prepareStatement(query);
							preparedStmt.setString(1, piLastName+", "+piFirstName.substring(0,1));
							result = preparedStmt.executeQuery();
							result.next();
							investigator_index__inv_id = result.getInt(1);
						}
						catch (Exception except) {;}	
					}

					if (investigator_index__inv_id == -1) {
						
					} else {
						//Check if project exists in DB
						String query = "SELECT p.PROJECT_NUMBER FROM  "+dbname+"project p left outer join  "+dbname+"investigator_index ii on ii.pid = p.id where p.PROJECT_NUMBER = ?"
								+ " and p.PROJECT_START_DATE = ? and p.PROJECT_END_DATE = ? and ii.inv_id = ?;";
						ResultSet result = null;
						try {
							PreparedStatement preparedStmt = conn.prepareStatement(query);
						
							preparedStmt.setString(1, project__PROJECT_NUMBER);
							preparedStmt.setString(2, project__PROJECT_START_DATE);
							preparedStmt.setString(3, project__PROJECT_END_DATE);
							preparedStmt.setString(4, String.valueOf(investigator_index__inv_id));

							result = preparedStmt.executeQuery();
							result.next();
							String number = result.getString(1);
							continue;
						}
						catch (Exception ex) {;}

					}

					String GetAgencySQL = "SELECT ID FROM  "+dbname+"agency_data WHERE AGENCY_FULL_NAME LIKE ?;";
					ResultSet rs7 = null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(GetAgencySQL);
						preparedStmt.setString(1, project__AGENCY_FULL_NAME);
						rs7 = preparedStmt.executeQuery();
						rs7.next();
						agency_index__aid = Integer.parseInt(rs7.getString(1));
					}
					catch (Exception e) {
					
					}


					String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__AGENCY_FULL_NAME.replaceAll("[\\n\\t\\r]"," "),
							String.valueOf(agency_index__aid),
							project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "), 
							project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
							project__PROJECT_START_DATE, project__PROJECT_END_DATE, 
							investigator_data__name.replaceAll("[\\n\\t\\r]"," "),
							institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "), 
							institution_data__city_name,states__states_abbrv,
							String.valueOf(institution_data__INSTITUTION_STATE), 
							institution_data__INSTITUTION_COUNTRY,
							String.valueOf(institution_index__inst_id),  
							String.valueOf(investigator_index__inv_id)};

					csvout.printRecord(output);
				}
				}
							
			csvout.close();
		}
	}
}