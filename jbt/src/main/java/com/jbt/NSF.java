package com.jbt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

/**
* This class parses the files related to the National Science Foundation.
* Returns String "NSF" when it done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the files retrieved from NIH ExPORTER website and put into specific data folders, and requires several parameters specified in the main Run class.
*/
public class NSF {

	/**
	 * This method calls all the parser associated with the National Science Foundation XML files.
	 * The files are typically loaded and unzipped into the Data/NSF folder.
	 * The folder path can be changed in the config file (typically, process.cfg).
	 * 
	 * @param inputfolder This is the folder where input files with project-related information are located (typically, Data/NSF).
	 * @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	 * @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
 	 * @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
 	 * @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	 * @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	 * @return			  String "NSF" to signify that the scraper is done running.
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	public static String nsfMain(String inputfolder, String outfolder, String host, String user, String passwd, String dbname) throws IOException,SAXException,ParserConfigurationException {
		/**
		* Opening one connection per class, as instructed. 
		*/
		Connection conn = MysqlConnect.connection(host,user,passwd);
		try {
			scrape(outfolder,inputfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The NSF files were not parsed correctly. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		
		MysqlConnect.closeConnection(conn);
		return "NSF";

	}
	
	/**
	 * This method reads through the input folders and generates list of files within them.
	 * @param dir	Directory specified by the method calling.
	 * @return List of files in the directory.
	 */
	public static File[] getFiles(String dir) {
		File[] files = new File(dir).listFiles();
		return files;
	}
	
	/**
	 * This method parses all project-related information from NSF files.
	 * 
	 * @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	 * @param inputfolder This is the folder where input files with project-related information are located (typically, Data/NSF).
	 * @param conn        Database connection initiated in the ahdbMain method.
     * @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	public static void scrape(String outfolder, String inputfolder, Connection conn, String dbname) throws IOException,SAXException,ParserConfigurationException {
		/**
		* The date is needed in every subclass for logging and file naming purposes given that a customized logger is implemented for the most transparent and easiest error handling and troubleshooting.
		*/
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		/**
		* As seen, the naming convention for the output files is class, method, and current date. This is needed to impose version control and easier data organization for FSRIO staff.
		*/
		CSVPrinter csvout = new CSVPrinter(new FileWriter(outfolder+"NSF_"+currentStamp+".csv"), CSVFormat.EXCEL.withDelimiter('\t'));
		
		/**
		* Different sources can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__PROJECT_OBJECTIVE", "project__PROJECT_START_DATE", "project__PROJECT_END_DATE",
				"institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ZIP", "institution_data__INSTITUTION_ADDRESS1", 
				"institution_data__INSTITUTION_STATE", "project__ACTIVITY_STATUS", "institution_data__INSTITUTION_NAME",
				"institution_data__INSTITUTION_CITY",
				"investigator_data__EMAIL_ADDRESS", "investigator_data__name", "institution_index__inst_id", "investigator_index__inv_id",
				"project__source_url", "agency_index__aid"};
		
		csvout.printRecord(header);
		
		/**
		 * Getting the list of files from abstracts inputfolder.
		 */
		File[] files = getFiles(inputfolder);
		for (File file : files) {
			if (file.getName().endsWith("xml")) {
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(file);
				
				/**
				 *  Get Award Information
				 */
				Element award = (Element) doc.getElementsByTagName("Award").item(0);
				
				/**
				* Every parser declares a list of variables that are present in the source files. 
				* It is important that different files and websites can have different lists of data fields. That explains why we do not template, extend and override.
				*/
				String project__PROJECT_TITLE = "";
				String project__PROJECT_NUMBER = "";
				String project__PROJECT_ABSTRACT = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project_awardInstrument = "";
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
				int institution_index__inst_id = -1; 
				int investigator_index__inv_id = -1;
				String institution_data__INSTITUTION_COUNTRY = "";
				String institution_data__INSTITUTION_STATE = ""; 
				int flag  = 0;
				
				if(award != null) {
					
					/**
					 * Project number	
					 */
					project__PROJECT_NUMBER = award.getElementsByTagName("AwardID").item(0).getTextContent();
					
					/**
					 * Project title and objective	
					 */
					project__PROJECT_TITLE = award.getElementsByTagName("AwardTitle").item(0).getTextContent();
					project__PROJECT_OBJECTIVE = award.getElementsByTagName("AbstractNarration").item(0).getTextContent().replace("<br/>"," ");
	
					String project__PROJECT_TITLE1 = project__PROJECT_TITLE.toLowerCase() + project__PROJECT_ABSTRACT.toLowerCase();
					
					/**
					 * Check whether this is a food safety project. If not, continue.
					 */
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
						
					
	
					/**
					 * Get years for start and end of award
					 */
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

					
					/**
					 *  Get Investigator information
					 */
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
		
		
					/**
					 *  Get Institution information
					 */
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
					
					/**
					* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
					* project number, project start date, project end date, institution names and/or PI name (if applicable).
					* This is exactly what the following MySQL queries are doing.
					*/
					
					institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
					investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name,institution_index__inst_id);
					institution_data__INSTITUTION_COUNTRY = MysqlConnect.GetCountrySQL(dbname, countries__COUNTRY_NAME.trim(), conn);
					institution_data__INSTITUTION_STATE = MysqlConnect.GetStateSQL(dbname, conn, states__states_abbrv);
					institution_data__INSTITUTION_STATE = institution_data__INSTITUTION_STATE.split("_")[0];
					
					/**
					 * Check project data by other fields in case institution and investigator data exist.
					 * If project does not exist then continue through the loop and do not write into the output spreadsheet.
					 */
					String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
					if (status.equals("Found")) continue;
							
					/**
					 *  Create variables that are needed in the tables, but havent been created so far
					 */
					String project__source_url ="http://www.nsf.gov/awardsearch/showAward?AWD_ID="+ project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," ") +"&HistoricalAwards=false";
					String agency_index__aid = "6";
					
					/**
					* Outputting all data into tab-separated file. 
					* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
					* Will be one institution per line.
					*/
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
