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

/**
* This class parses the files related to the National Institutes of Health.
* Returns String "NIH" when it done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the files retrieved from NIH ExPORTER website and put into specific data folders, and requires several parameters specified in the main Run class.
*/
public class NIH {
	/**
	 * NIH ExPORTER provides abstracts and other project-related information in two separate files. We have to match the two to make the parsing complete for the FSRIO Research Projects Database.
	 */
	HashMap<String, String> abstracts = new HashMap<String, String>();
	HashMap<String, String> indexMap = new HashMap<String, String>();
	
	/**
	 * This method calls all the parser associated with the National INstitutes of Health files.
	 * The files are typically loaded and unzipped into the Data/NIH and Data/NIH/Abstracts folders
	 * The folder path can be changed in the config file (typically, process.cfg).
	 * 
	 * @param inputfolder This is the folder where input files with project-related information are located (typically, Data/NIH).
	 * @param inputfolder_abstracts This is the folder where input files with project abstracts are located (typically, Data/NIH/Abstracts).
	 * @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	 * @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
 	 * @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
 	 * @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	 * @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	 * @return 			  String "NIH" to signify that the scraper is done running.
	 * @throws			  IOException
	 */
	public static String nihMain(String inputfolder, String inputfolder_abstracts, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		
		NIH obj  =  new NIH();
		/**
		* Opening one connection per class, as instructed. 
		*/
		Connection conn = MysqlConnect.connection(host,user,passwd);
		try {
			obj.abstracts(inputfolder_abstracts);
		} catch (Exception ex) {
			System.out.println("Warning: The NIH abstracts were not parsed correctly. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		try {
			obj.projects(outfolder,inputfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The NIH project-related information files were not parsed correctly. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		
		MysqlConnect.closeConnection(conn);
		return "NIH";
	
	}

	/**
	 * This method reads through the input folders and generates list of files within them.
	 * @param dir	Directory specified by the method calling.
	 * @return List of files in the directory.
	 */
	public File[] getFiles(String dir) {
		File[] files = new File(dir).listFiles();

		return files;
	}

	/**
	 * This method parses the abstract information on projects.
	 * @param inputfolder_abstracts This is the folder where input files with project abstracts are located (typically, Data/NIH/Abstracts).
	 * @throws IOException
	 */
	public void abstracts(String inputfolder_abstracts) throws IOException {
		/**
		 * Getting the list of files from abstracts inputfolder.
		 */
		File[] files = getFiles(inputfolder_abstracts);
		for (File file : files) {

			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(new FileReader(file));
			if (!file.getName().contains(".csv")) {
				continue;
			}
			for (CSVRecord record : records) {
					try {
						abstracts.put(record.get("APPLICATION_ID"), record.get("ABSTRACT_TEXT"));
					}
					catch (Exception e) {
						System.out.println("WARNING: The file "+file+" does not contain the correct header Column 1: APPLICATION_ID and Column 2: ABSTRACT_TEXT."
								+ " Please check the mentioned spreadsheet and if it looks okay, add the specified header to the first row; then re-run.");
					}
			}
		}
	}
	/**
	 * This method parses all project-related information except abstracts.
	 * 
	 * @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	 * @param inputfolder This is the folder where input files with project-related information are located (typically, Data/NIH).
	 * @param conn        Database connection initiated in the ahdbMain method.
     * @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	 * @throws IOException
	 */
	public void projects(String outfolder, String inputfolder,Connection conn, String dbname) throws IOException {
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
		CSVPrinter csvout = new CSVPrinter(new FileWriter(outfolder+"NIH_"+currentStamp+".csv"), CSVFormat.EXCEL.withDelimiter('\t'));
		
		/**
		* Different sources can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__AGENCY_FULL_NAME", "agency_index__aid", 
				"project__PROJECT_OBJECTIVE", "project__PROJECT_TITLE",
				"project__PROJECT_START_DATE", "project__PROJECT_END_DATE",
				"investigator_data__name",
				"institution_data__INSTITUTION_NAME", "institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_STATE", "institution_data__INSTITUTION_COUNTRY",
				"institution_index__inst_id", 
				"investigator_index__inv_id"};
		csvout.printRecord(header);
		/**
		 * Getting the list of files from abstracts inputfolder.
		 */
		File[] files = getFiles(inputfolder);
		for (File file : files) {
			if (!file.getName().contains(".csv")) {
				continue;
			}
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(new FileReader(file));
			for (CSVRecord record : records) {
				/**
				* Every parser declares a list of variables that are present in the source files. 
				* It is important that different files and websites can have different lists of data fields. That explains why we do not template, extend and override.
				*/
				String project__AGENCY_FULL_NAME = "";
				String project__PROJECT_OBJECTIVE= "";
				String project__PROJECT_TITLE = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String investigator_data__name = "";
				String project__PROJECT_NUMBER = "";
				String institution_data__INSTITUTION_NAME = "";
				String institution_data__city_name = "";
				String states__states_abbrv = "";
				String comment = "";
				int institution_index__inst_id = -1; 
				String institution_data__INSTITUTION_STATE = ""; 
				int investigator_index__inv_id = -1;
				int agency_index__aid = -1;
				String institution_data__INSTITUTION_COUNTRY = ""; 
				/**
				 * Check if it is food safety at all before proceeding. This is done with the NIH Spending Categories.
				 */
				String foodFlag = record.get("NIH_SPENDING_CATS");
				if (!foodFlag.toLowerCase().contains("food safety")) {
					continue;
				}
				
				/**
				 *  Project information
				 */
				project__AGENCY_FULL_NAME = WordUtils.capitalize(record.get("IC_NAME").toLowerCase())
						.replace("Of","of").replace("And", "and")
						.replace("&", "and").replace("Eunice Kennedy Shriver ", "").replace("Lung, and","Lung and");
				project__PROJECT_OBJECTIVE = abstracts.get(record.get("APPLICATION_ID"))
						.replace("DESCRIPTION (provided by applicant): ","")
						.replace("Description (provided by the applicant): ", "")
						.replace("Seeinstructions): ", "")
						.replace("DESCRIPTION (provided by applicant)","").trim();
				project__PROJECT_TITLE = WordUtils.capitalize(record.get("PROJECT_TITLE"),' ','-');
				project__PROJECT_START_DATE = record.get("PROJECT_START");
				project__PROJECT_END_DATE = record.get("PROJECT_END");
				project__PROJECT_NUMBER = record.get("FULL_PROJECT_NUM");
				
				try {
					project__PROJECT_START_DATE = project__PROJECT_START_DATE.substring(project__PROJECT_START_DATE.length()-4);
				}
				catch(Exception e ) {
					/**
					 * There is no information in the file - so, the exception should be ignored; it will be obvious in the QA spreadsheet.
					 */
					//System.out.println("WARNING: The project start date did not parse correctly; please check in the file "+file+" under FULL_PROJECT_NUM "+project__PROJECT_NUMBER);
				}
				
				try {
					project__PROJECT_END_DATE = project__PROJECT_END_DATE.substring(project__PROJECT_END_DATE.length()-4);	
				}
				catch (Exception e) {
					/**
					 * There is no information in the file - so, the exception should be ignored; it will be obvious in the QA spreadsheet.
					 */

					//System.out.println("WARNING: The project end date did not parse correctly; please check in the file "+file+" under FULL_PROJECT_NUM "+project__PROJECT_NUMBER);
				}
				
				/**
				 *  Get Institution information
				 */
				institution_data__INSTITUTION_NAME = WordUtils.capitalize(record.get("ORG_NAME").toLowerCase(),' ','-','/')
						.replace("Of","of").replace("And", "and");
				institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
				institution_data__city_name = WordUtils.capitalize(record.get("ORG_CITY").toLowerCase(),' ','-');
				institution_data__INSTITUTION_STATE = record.get("ORG_STATE");
				institution_data__INSTITUTION_COUNTRY = WordUtils.capitalize(record.get("ORG_COUNTRY").toLowerCase(),' ','-');
				
				if (institution_index__inst_id == -1) {
					comment = "Please populate institution fields by exploring the institution named on the project.";
				}

				institution_data__INSTITUTION_COUNTRY = MysqlConnect.GetCountrySQL(dbname, institution_data__INSTITUTION_COUNTRY.trim(), conn);
				if (institution_data__INSTITUTION_COUNTRY.equals("")) {
					/**
					 *  Country does not exist in DB --> comment: "Check country field"
					 */
					comment = "Please check the country name and respective index in the DB - might be a spelling mistake or new country.";
				}
				
				if (institution_data__INSTITUTION_COUNTRY.equals("1")) {
					institution_data__INSTITUTION_STATE = MysqlConnect.GetStateSQL(dbname, conn, institution_data__INSTITUTION_STATE.trim());
				}
				if (institution_data__INSTITUTION_STATE.equals("")) {
					/**
					 * Add to comment field rather than just have it there re-write other comments
					 */
					if (comment.equals("")) {
						comment = " Please check the address information in the file "+ file+" to see whether state field is present on project "+project__PROJECT_NUMBER;
					} else {
						comment += " Please check the address information in the file "+ file+" to see whether state field is present on project "+project__PROJECT_NUMBER;
					}
				}
				
				/**
				 *  Get Investigator information. Could be one to many PIs.
				 */
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

					/**
					 *  Let us see if we can find the investigator in the already existing data. 
					 */
					
					investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name);
					/**
					 * Check project data by other fields in case institution and investigator data exist.
					 * If project does not exist then continue through the loop and do not write into the output spreadsheet.
					 */
					String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
					if (status.equals("Found")) continue;
					/**
					 * Check agency ID from the FSRIO DB.
					 */
					agency_index__aid = MysqlConnect.GetAgencySQL(dbname, conn, project__AGENCY_FULL_NAME.trim(), agency_index__aid);
					
					/**
					* Outputting all data into tab-separated file. 
					* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
					* Will be one institution per line.
					*/
					String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__AGENCY_FULL_NAME.replaceAll("[\\n\\t\\r]"," "),
							String.valueOf(agency_index__aid),
							project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "), 
							project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
							project__PROJECT_START_DATE, project__PROJECT_END_DATE, 
							investigator_data__name.replaceAll("[\\n\\t\\r]"," "),
							institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "), 
							institution_data__city_name,
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