package com.jbt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.IOException;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.text.WordUtils;
import org.w3c.dom.*;

import javax.xml.parsers.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class NSF {
	
	public static String nsfMain(String inputfolder, String outfolder, String host, String user, String passwd, String dbname) throws IOException,SAXException,ParserConfigurationException {
		Connection conn = MysqlConnect.connection(host,user,passwd);
		scrape(outfolder,inputfolder,conn,dbname);
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}	
		return "NSF";

	}
	
	public static File[] getFiles(String dir) {
		File[] files = new File(dir).listFiles();
		return files;
	}

	public static void scrape(String outfolder, String inputfolder, Connection conn, String dbname) throws IOException,SAXException,ParserConfigurationException {
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVPrinter csvout = new CSVPrinter(new FileWriter(outfolder+"NSF_"+currentStamp+".csv"), CSVFormat.EXCEL.withDelimiter('\t'));

		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__PROJECT_OBJECTIVE", "project__PROJECT_START_DATE", "project__PROJECT_END_DATE",
				"institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ZIP", "institution_data__INSTITUTION_ADDRESS1", 
				"institution_data__INSTITUTION_STATE", "project__ACTIVITY_STATUS", "institution_data__INSTITUTION_NAME",
				"institution_data__INSTITUTION_CITY",
				"investigator_data__EMAIL_ADDRESS", "investigator_data__name", "institution_index__inst_id", "investigator_index__inv_id",
				"project__source_url", "agency_index__aid"};
		
		csvout.printRecord(header);

		File[] files = getFiles(inputfolder);
		for (File file : files) {
			if (file.getName().endsWith("xml")) {
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(file);
				//doc.getDocumentElement().normalize();
	
				// Get Award Information
				Element award = (Element) doc.getElementsByTagName("Award").item(0);
				
				//Needed variables
				String project__PROJECT_TITLE = "";
				String project__PROJECT_NUMBER = "";
				String project__PROJECT_FUNDING = "";
				String project__PROJECT_ABSTRACT = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project_awardInstrument = "";
				String project__LAST_UPDATE = "";
				String project__DATE_ENTERED = "";
				int project__ACTIVITY_STATUS = 0;
				String project__PROJECT_OBJECTIVE = "";
				String institution_data__INSTITUTION_ZIP = "";
				String institution_data__INSTITUTION_ADDRESS1 = "";
				String comment = "";
				Date dateNow = null;
				String institution_data__INSTITUTION_NAME = "";
				String institution_data__INSTITUTION_CITY = "";
				String states__state_name = "";
				String countries__COUNTRY_NAME = "";
				String states__states_abbrv = "";
				String investigator_data__EMAIL_ADDRESS = "";
				String investigator_data__name = "";
				int flag  = 0;
				
				if(award != null) {
					
					//Project number	
					project__PROJECT_NUMBER = award.getElementsByTagName("AwardID").item(0).getTextContent();
					
						
					project__PROJECT_TITLE = award.getElementsByTagName("AwardTitle").item(0).getTextContent();
					project__PROJECT_OBJECTIVE = award.getElementsByTagName("AbstractNarration").item(0).getTextContent().replace("<br/>"," ");
	
					String project__PROJECT_TITLE1 = project__PROJECT_TITLE.toLowerCase() + project__PROJECT_ABSTRACT.toLowerCase();
					
					if(!(project__PROJECT_TITLE1.contains("food safety") || project__PROJECT_TITLE1.contains("feed safety") || 
							project__PROJECT_TITLE1.contains("food protection") || project__PROJECT_TITLE1.contains("foodborne") || 
							project__PROJECT_TITLE1.contains("salmonella") || project__PROJECT_TITLE1.contains("escherichia coli") || 
							project__PROJECT_TITLE1.contains("food defense") || project__PROJECT_TITLE1.contains("food regulatory") || 
							project__PROJECT_TITLE1.contains("produce and safety") || project__PROJECT_TITLE1.contains("seafood") ||
							project__PROJECT_TITLE1.contains("BOVINE SPONGIFORM ENCEPHALOPATHY".toLowerCase()) || 
							project__PROJECT_TITLE1.contains("shellfish") || 
							project__PROJECT_TITLE1.contains("fish product") || project__PROJECT_TITLE1.contains("fish oil") ||
							project__PROJECT_TITLE1.contains("avian influenza") || 
							(project__PROJECT_TITLE1.contains("food") && project__PROJECT_TITLE1.contains("safety")) || 
							(project__PROJECT_TITLE1.contains("campylobacter") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("clostridium") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("cryptosporidium") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("salmonella") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("shigella") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("listeria") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("staphylococcus") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("vibrio")  && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("hepatitis") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("cyclospora") && project__PROJECT_TITLE1.contains("food")) || 
							(project__PROJECT_TITLE1.contains("seafood")  && project__PROJECT_TITLE1.contains("safety")) || 
							(project__PROJECT_TITLE1.contains("food") && project__PROJECT_TITLE1.contains("processing")) || 
							project__PROJECT_TITLE1.contains("food Regulations") || 
							(project__PROJECT_TITLE1.contains("beef") && project__PROJECT_TITLE1.contains("safety")) ||
							(project__PROJECT_TITLE1.contains("e. coli") && project__PROJECT_TITLE1.contains("food")) ||
							project__PROJECT_TITLE1.contains("food packaging") || project__PROJECT_TITLE1.contains("food analysis") ||
							project__PROJECT_TITLE1.contains("food systems") || project__PROJECT_TITLE1.contains("food standards") || 
							project__PROJECT_TITLE1.contains("food additives")))
							continue;
						
					
	
						project__PROJECT_FUNDING = award.getElementsByTagName("AwardAmount").item(0).getTextContent();
						
						//Get years for start and end of award
						Pattern patDate = Pattern.compile("(\\d+)$");
						String projStart = award.getElementsByTagName("AwardEffectiveDate").item(0).getTextContent();
						project__PROJECT_START_DATE = projStart.substring(projStart.length()-4);
						String projEnd = award.getElementsByTagName("AwardExpirationDate").item(0).getTextContent();
						project__PROJECT_END_DATE = projEnd.substring(projEnd.length()-4);
						
						project_awardInstrument = award.getElementsByTagName("AwardInstrument").item(0).getTextContent();
		
						dateNow = new Date();
						Date dateProj = new Date(projEnd);
						long diff = dateNow.getTime() - dateProj.getTime();
						long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
						if (days >= 0) {
							if(project_awardInstrument.toLowerCase().contains("continuing"))
								project__ACTIVITY_STATUS = 5;
							else
								project__ACTIVITY_STATUS = 1;
						}
						else
							project__ACTIVITY_STATUS = 4;
	
					
					project__LAST_UPDATE = dateFormat.format(current);
					DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
					project__DATE_ENTERED = dateFormatEnter.format(current);
		
					// Get Investigator information
					Element investigator = (Element) doc.getElementsByTagName("Investigator").item(0);
					String FirstName = null;
					String LastName = null;
					
					
					if(investigator != null) {
						FirstName = investigator.getElementsByTagName("FirstName").item(0).getTextContent();
						LastName = investigator.getElementsByTagName("LastName").item(0).getTextContent();
						investigator_data__EMAIL_ADDRESS = investigator.getElementsByTagName("EmailAddress").item(0).getTextContent();
					}
					if(LastName != null && FirstName != null) 
						investigator_data__name = LastName + ", " + FirstName;
		
		
					// Get Institution information
					Element institution = (Element) doc.getElementsByTagName("Institution").item(0);

					if(institution != null) {
						institution_data__INSTITUTION_NAME = institution.getElementsByTagName("Name").item(0).getTextContent();
						institution_data__INSTITUTION_CITY = institution.getElementsByTagName("CityName").item(0).getTextContent();
						states__state_name = institution.getElementsByTagName("StateName").item(0).getTextContent();
						states__states_abbrv = institution.getElementsByTagName("StateCode").item(0).getTextContent();
						countries__COUNTRY_NAME = institution.getElementsByTagName("CountryName").item(0).getTextContent();
						institution_data__INSTITUTION_ZIP = institution.getElementsByTagName("ZipCode").item(0).getTextContent().substring(0,4);
						institution_data__INSTITUTION_ADDRESS1 = institution.getElementsByTagName("StreetAddress").item(0).getTextContent();
					}
		
		
					// See if the institution ID exists, if not make it -1 to reflect we need to add.
					int institution_index__inst_id = -1; 
					String GetInstIDsql = "SELECT ID FROM " + dbname + ".institution_data WHERE INSTITUTION_NAME = \"" +  institution_data__INSTITUTION_NAME + "\";";
					ResultSet rs = MysqlConnect.sqlQuery(GetInstIDsql,conn);
					try {
						rs.next();
						institution_index__inst_id = Integer.parseInt(rs.getString(1));
					}
					catch (Exception e) {

					}

		
					// See if the country ID exists, if not make it -1 to reflect we need to add.
					int institution_data__INSTITUTION_COUNTRY = -1; 
					String GetcountryIDsql = "SELECT ID FROM " + dbname + ".countries WHERE COUNTRY_NAME = \"" +  countries__COUNTRY_NAME.trim() + "\";";
					ResultSet rs2 = MysqlConnect.sqlQuery(GetcountryIDsql,conn);
					try {
						rs2.next();
						institution_data__INSTITUTION_COUNTRY = Integer.parseInt(rs2.getString(1));
					}
					catch (Exception e) {
					}

		
					// See if the state ID exists, if not make it -1 to reflect we need to add.
					int institution_data__INSTITUTION_STATE = -1; 
					String GetstateIDsql = "SELECT ID FROM " + dbname + ".states WHERE abbrv = \"" +  states__states_abbrv + "\";";
					ResultSet rs3 = MysqlConnect.sqlQuery(GetstateIDsql,conn);
					try {
						rs3.next();
						institution_data__INSTITUTION_STATE = Integer.parseInt(rs3.getString(1));
					}
					catch (Exception e) {
						;
					}

					// Determining project type.
					int projecttype__ID = 999;
					if(project_awardInstrument.toLowerCase().contains("grant")) {
						projecttype__ID = 3;
					}
					else {
						// Checking to see if project type exists in projecttype table.
						String GetProjectTypeIDSQL = "SELECT ID FROM " + dbname + ".projecttype WHERE NAME LIKE \"" +  project_awardInstrument + "\";";
						ResultSet rs4 = MysqlConnect.sqlQuery(GetstateIDsql,conn);
						try {
							rs4.next();
							projecttype__ID = Integer.parseInt(rs4.getString(1));
						}
						catch (Exception e) {
							;
						}

					}
		
					// Let us see if we can find the investigator in the already existing data. 
					// Condition: investigator must belong to the same institution that we just parsed.
					// We first use email, then name.
					int investigator_index__inv_id = -1;
					String GetInvestigatorSQL = "SELECT ID FROM " + dbname + ".investigator_data WHERE EMAIL_ADDRESS LIKE \"" +  investigator_data__EMAIL_ADDRESS + "\";";
					ResultSet rs5 = MysqlConnect.sqlQuery(GetInvestigatorSQL,conn);
					try {
						rs5.next();
						investigator_index__inv_id = Integer.parseInt(rs5.getString(1));
					}
					catch (Exception e) {
						GetInvestigatorSQL = "SELECT ID FROM " + dbname + ".investigator_data WHERE NAME LIKE \"" +  investigator_data__name + "\";";
						ResultSet rs6 = MysqlConnect.sqlQuery(GetInvestigatorSQL,conn);
						try {
							rs6.next();
							investigator_index__inv_id = Integer.parseInt(rs6.getString(1));
						}
						catch (Exception ee) {
							String query = "SELECT * FROM "+dbname+".investigator_data where name regexp \"^"+LastName+", "+FirstName.substring(0,1)+"\"";
							ResultSet result = MysqlConnect.sqlQuery(query,conn);
							try {
								result.next();
								investigator_index__inv_id = result.getInt(1);
							}
							catch (Exception except) {;}	
						}
					}

					
					if (investigator_index__inv_id == -1) {
						
					} else {

						//Check if project exists in DB
						String query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p left outer join "+dbname+".investigator_index ii on ii.pid = p.id where p.PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\""
								+ " and p.PROJECT_START_DATE = \""+project__PROJECT_START_DATE+"\" and p.PROJECT_END_DATE = \""+project__PROJECT_END_DATE+"\" and ii.inv_id = \""+String.valueOf(investigator_index__inv_id)+"\"";
						ResultSet result = MysqlConnect.sqlQuery(query,conn);
						try {
							result.next();
							String number = result.getString(1);
							continue;
						}
						catch (Exception ex) {;}

					
					}
					
					// Create variables that are needed in the tables, but havent been created so far
					String project__source_url ="http://www.nsf.gov/awardsearch/showAward?AWD_ID="+ project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," ") +"&HistoricalAwards=false";
					String agency_index__aid = "6";
					String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
							project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "), project__PROJECT_START_DATE, project__PROJECT_END_DATE,
							String.valueOf(institution_data__INSTITUTION_COUNTRY),
							institution_data__INSTITUTION_ZIP.replaceAll("[\\n\\t\\r]"," "), institution_data__INSTITUTION_ADDRESS1.replaceAll("[\\n\\t\\r]"," "), 
							String.valueOf(institution_data__INSTITUTION_STATE), String.valueOf(project__ACTIVITY_STATUS), institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
							WordUtils.capitalize(institution_data__INSTITUTION_CITY.toLowerCase().replaceAll("[\\n\\t\\r]"," "),' ','-'), 
							investigator_data__EMAIL_ADDRESS.replaceAll("[\\n\\t\\r]"," "), investigator_data__name.replaceAll("[\\n\\t\\r]"," "), String.valueOf(institution_index__inst_id), String.valueOf(investigator_index__inv_id),
							project__source_url, agency_index__aid};

					csvout.printRecord(output);
					
				}
			}
		}
		csvout.close();

	}
}
