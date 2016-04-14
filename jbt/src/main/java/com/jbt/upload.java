package com.jbt;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import net.sf.junidecode.Junidecode;

/**
 * This class uploads the data into FSRIO Research Projects Database after the QA is done on tab-separated files from scraping and parsing.
 *
 */
public class upload {
	/**
	 * Dates are defined for LAST_UPDATE and DATE_ENTERED fields.
	 */
	static Date current = new Date();
	static DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
	static String currentStamp = dateFormatCurrent.format(current);
	
	/**
	 * This method is called from the Run class to upload files one by one
	 *  
	* @param Filename    Name of the file to be uploaded
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.

	 */
	public static void mainUpload(File Filename, String host, String user, String passwd, String dbname, String logfile)  {
		System.out.println("Working on "+Filename);
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		Connection conn = MysqlConnect.connection(host,user,passwd);
		uploadRecords(Filename,conn, dbname);
		MysqlConnect.closeConnection(conn);		
	}

	/**
	 * This method opens a connection the database, loops through the csv files, and uploads new records. 
	 *  
	 * @param fileName    Name of the file to be uploaded
	 * @param conn        Database connection initiated in the mainUpload method.
	 * @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.

	 */
	public static void uploadRecords(File fileName, Connection conn, String dbname)  {
		MultiMap PIS = new MultiValueMap();
		MultiMap INSTITUTIONS = new MultiValueMap();
		Iterable<CSVRecord> records = null;
		try {
			records = CSVFormat.EXCEL.withHeader().withDelimiter('\t').parse(new FileReader(fileName));
		}
		catch(IOException e) {
			System.out.println("WARNING: The file "+fileName+" was not found. Please make sure that the file exists.");
		}
	
		for (CSVRecord record : records) {
			String project__PROJECT_NUMBER = "";
			String project__PROJECT_TITLE = "";
			/**
			 * In case the project number is missing, we use the project title. 
			 */
			ResultSet dummy = null;
			project__PROJECT_NUMBER = recordGet("project__PROJECT_NUMBER", record,dummy);
			if(project__PROJECT_NUMBER.equals("")) project__PROJECT_TITLE = recordGet("project__PROJECT_TITLE", record,dummy);
			/**
			* In case both project number and title are missing, we just leave it blank
			*/
			String institution_index__inst_id = "";
			String investigator_index__inv_id= "";
			String investigator_data__name ="";
			String end = "";
			institution_index__inst_id = recordGet("institution_index__inst_id", record,dummy);
			investigator_index__inv_id = recordGet("investigator_index__inv_id", record,dummy);

			/**
			 * Based on FSRIO requirements, we first try to get investigator name and institution name combo, however
			 * if institution name is missing, we check against all PIs
			 */
			
			/**
			 * We use edit distance to see if the institutions and investigators exist in the database.
			 * If they exist, we get their IDs, if not, we add them to the database and use the newly assigned ID. 
			 */
			if (institution_index__inst_id.equals("-1"))  institution_index__inst_id = checkAddInst(record, conn, dbname);
			if (investigator_index__inv_id.equals("-1")) investigator_index__inv_id = getPIid(record, institution_index__inst_id, conn, dbname);
			
			/**
			 * See if the project already exists in the database.
			 */			
			String query = "";
			ResultSet result = null;
			
			if (project__PROJECT_NUMBER.equals("")) {
				query = "SELECT * FROM  "+dbname+".project p left join "
						+ " "+dbname+".institution_index inst on inst.pid =  p.id left join "
						+ "  "+dbname+".investigator_index inv on inv.pid = p.id where PROJECT_TITLE = ? "
						+ " order by date_entered desc limit 1;";
				String a[] = {project__PROJECT_TITLE};
				result = MysqlConnect.uploadSQLResult(a, conn, query);
			} else {
				query = "SELECT * FROM  "+dbname+".project p left join "
						+ " "+dbname+".institution_index inst on inst.pid =  p.id left join "
						+ "  "+dbname+".investigator_index inv on inv.pid = p.id where PROJECT_NUMBER = ? "
						+ " order by date_entered desc limit 1;";
				String a[] = {project__PROJECT_NUMBER};
				result = MysqlConnect.uploadSQLResult(a, conn, query);
			}
			
			/**
			 * Can be multiple PIs and institutions per project
			 */
			HashMap<String,String> inst = new HashMap<String,String>();
			HashMap<String,String> investigator = new HashMap<String,String>();
			
			String t = "";
			int flag = 0;
			while (true) {
				try {
					result.next();
					t = result.getString("ID");
					investigator.put(result.getString("inv_id"),t);
					inst.put(result.getString("inst_id"),t);
					
					if (!t.isEmpty() && !t.equals("")) {
						flag = 1;
						updateRecord(record, result,conn, dbname);
					}
				} catch (Exception ee) {break;}
			} 
			
			if (flag == 0) {
				insertRecord(record, institution_index__inst_id, investigator_index__inv_id, conn, dbname);
			}

			
			if(!investigator_index__inv_id.equals("-1") 
					&& !investigator_index__inv_id.equals("") && !Arrays.asList(investigator.keySet()).contains(investigator_index__inv_id)
					&& !t.isEmpty() && !t.equals("")) {
					/**
					 *  Add the new PI information to a dictionary, but only if it is not -1. 
					 */
					System.out.println(investigator);
					System.out.println(investigator_index__inv_id);
					System.out.println(t);
					
					PIS.put(t,investigator_index__inv_id);
						
			}
			if (!institution_index__inst_id.equals("-1") 
					&& !institution_index__inst_id.equals("") && !Arrays.asList(inst.keySet()).contains(institution_index__inst_id)
					&& !t.isEmpty() && !t.equals("")) {
						/**
						 *  Add the new Inst information to a dictionary, but only if it is not -1. 
						 */
					INSTITUTIONS.put(t,institution_index__inst_id);
			}
		}
		/**
		 * Now we can update the index tables with all new PI and Institution information.  
		 */
		/**
		 * PI TABLE
		 */
		Set<String> keys = PIS.keySet();
		for(String s : keys) {
			ArrayList<String> list = (ArrayList<String>)(PIS.get(s));
			if(list == null) continue;
			if(list.isEmpty()) continue;
			Set<String> values = new HashSet<String>(list);  
			for (String s2: values) {
				String insertQuery = "INSERT INTO   "+dbname+".investigator_index (pid, inv_id)"
						+ " VALUES (?, ?);";
				String arr[] = {s, s2};
				MysqlConnect.uploadSQL(arr, conn, insertQuery);
			}

		}

		/**
		 * INSTITUTION TABLE
		 */
		keys = INSTITUTIONS.keySet();
		for(String s : keys) {
			ArrayList<String> list = (ArrayList<String>)(PIS.get(s));
			if(list == null) continue;
			if(list.isEmpty()) continue;
			Set<String> values = new HashSet<String>(list);  
			for (String s2: values) {
				String insertQuery = "INSERT INTO   "+dbname+".institution_index (pid, inst_id)"
						+ " VALUES (?, ?);";
				String arr[] = {s, s2};
				MysqlConnect.uploadSQL(arr, conn, insertQuery);
			}

		}
	}
	/**
	 * This method opens a inserts a new record into the database.  
	 *  
	* @param record                            Record to be inserted
	* @param institution_index__inst_id        The institution id as found in the CSV file/ determined using database
	* @param institution_index__inst_id        The investigator id as found in the CSV file determined using database
	* @param conn                              Database connection initiated in the mainUpload method.
	* @param dbname                            Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.

	 */
	public static void insertRecord(CSVRecord record, String institution_index__inst_id, String investigator_index__inv_id, Connection conn, String dbname) {
		String insertQuery = "INSERT INTO   "+dbname+".project (PROJECT_NUMBER, PROJECT_TITLE, source_url, "
				+ "PROJECT_START_DATE, PROJECT_END_DATE, PROJECT_FUNDING, PROJECT_TYPE, "
				+ "PROJECT_KEYWORDS, PROJECT_IDENTIFIERS, PROJECT_COOPORATORS, PROJECT_ABSTRACT, "
				+ "PROJECT_PUBLICATIONS, other_publications, PROJECT_MORE_INFO, PROJECT_OBJECTIVE,"
				+ "PROJECT_ACCESSION_NUMBER, ACTIVITY_STATUS, DATE_ENTERED, COMMENTS,"
				+ "archive,LAST_UPDATE,  LAST_UPDATE_BY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		String PROJECT_NUMBER = null;
		String PROJECT_TITLE = "";
		String source_url = "";
		String PROJECT_START_DATE = "";
		String PROJECT_END_DATE = "";	
		String PROJECT_FUNDING = "";
		String PROJECT_TYPE = "";
		String PROJECT_KEYWORDS = null;
		String PROJECT_IDENTIFIERS = null;
		String  PROJECT_COOPORATORS = null;	
		String  PROJECT_ABSTRACT = "";	
		String  PROJECT_PUBLICATIONS = null;	
		String  other_publications = "";
		String  PROJECT_MORE_INFO = null;
		String  PROJECT_ACCESSION_NUMBER = null;
		String  ACTIVITY_STATUS = "";
		String  DATE_ENTERED = null;	
		String  COMMENTS = null;
		String  archive = "1";
		String  LAST_UPDATE = null;	
		String  LAST_UPDATE_BY = "air";
		String PROJECT_OBJECTIVE= "";
		
		/**
		 * Extract values from the record to create a query.
		 */
		ResultSet dummy = null;
		PROJECT_NUMBER = recordGet("project__PROJECT_NUMBER", record,dummy);
		PROJECT_TITLE = recordGet("project__PROJECT_TITLE", record,dummy);
		source_url = recordGet("project__source_url", record,dummy);
		PROJECT_START_DATE = recordGet("project__PROJECT_START_DATE", record,dummy);
		PROJECT_END_DATE = recordGet("project__PROJECT_END_DATE", record,dummy);
		PROJECT_FUNDING = recordGet("project__PROJECT_FUNDING", record,dummy);
		PROJECT_TYPE = recordGet("project__PROJECT_TYPE", record,dummy);
		PROJECT_KEYWORDS = recordGet("project__PROJECT_KEYWORDS", record,dummy);
		PROJECT_IDENTIFIERS = recordGet("project__PROJECT_IDENTIFIERS", record,dummy);
		PROJECT_COOPORATORS = recordGet("project__PROJECT_COOPORATORS", record,dummy);
		PROJECT_ABSTRACT = recordGet("project__PROJECT_ABSTRACT", record,dummy);
		PROJECT_PUBLICATIONS = recordGet("project__PROJECT_PUBLICATIONS", record,dummy);
		other_publications = recordGet("project__other_publications", record,dummy);
		PROJECT_MORE_INFO = recordGet("project__PROJECT_MORE_INFO", record,dummy);
		PROJECT_ACCESSION_NUMBER = recordGet("project__PROJECT_ACCESSION_NUMBER", record,dummy);
		PROJECT_OBJECTIVE = recordGet("project__PROJECT_OBJECTIVE", record,dummy);
		ACTIVITY_STATUS = recordGet("project__ACTIVITY_STATUS", record,dummy);
		DATE_ENTERED = currentStamp;
		COMMENTS = recordGet("project__COMMENTS", record,dummy);
		archive = recordGet("project__archive", record,dummy);
		LAST_UPDATE = currentStamp;
		LAST_UPDATE_BY = recordGet("project__LAST_UPDATE_BY", record,dummy);

			
		if (PROJECT_START_DATE.trim().equals("")) PROJECT_START_DATE = null;
		if (PROJECT_END_DATE.trim().equals("")) PROJECT_END_DATE = null;
		if (PROJECT_FUNDING.trim().equals("")) PROJECT_FUNDING = null;
		if (PROJECT_TYPE.trim().equals("")) PROJECT_TYPE = null;
		if (ACTIVITY_STATUS.trim().equals("")) ACTIVITY_STATUS = null;
		if (archive.trim().equals("")) archive = "1";
		
		
		String arr[] = {PROJECT_NUMBER, Junidecode.unidecode(PROJECT_TITLE), source_url,PROJECT_START_DATE,PROJECT_END_DATE,
				PROJECT_FUNDING, PROJECT_TYPE, PROJECT_KEYWORDS, PROJECT_IDENTIFIERS, PROJECT_COOPORATORS,
				Junidecode.unidecode(PROJECT_ABSTRACT), PROJECT_PUBLICATIONS, other_publications, PROJECT_MORE_INFO, 
				Junidecode.unidecode(PROJECT_OBJECTIVE),PROJECT_ACCESSION_NUMBER, ACTIVITY_STATUS, DATE_ENTERED,
				COMMENTS, archive, LAST_UPDATE, LAST_UPDATE_BY};
		MysqlConnect.uploadSQL(arr, conn, insertQuery);
		/**
		 * Get the ID for the project just inserted
		 */
		String query = "SELECT ID from  "+dbname+".project order by ID desc limit 1;";
		ResultSet result = null;
		String ID = "";
		String a[] = {};
		result = MysqlConnect.uploadSQLResult(a, conn, query);
		
		try {
			result.next();
			ID = result.getString("ID");
		}
		catch(Exception e) {;}		
		
		if (!investigator_index__inv_id.equals("-1")) {
			/**
			 *  Insert the new project into investigator_index
			 */
			insertQuery = "INSERT INTO  "+dbname+".investigator_index (pid, inv_id)"
					+ " VALUES (?, ?);";
			String arr2 [] = {ID, investigator_index__inv_id};
			if (!investigator_index__inv_id.equals("")) MysqlConnect.uploadSQL(arr2, conn, insertQuery);

		}
		if (!institution_index__inst_id.equals("-1")) {
			/**
			 *  Insert the new project into institution_index
			 */
			insertQuery = "INSERT INTO   "+dbname+".institution_index (pid, inst_id)"
					+ " VALUES (?, ?);";
			String arr1[] = {ID, institution_index__inst_id};
			if (!institution_index__inst_id.equals("")) MysqlConnect.uploadSQL(arr1, conn, insertQuery);
		}
				
	}
	/**
	 * This method updates an existing record in the database with the new values that are read in from the CSV  
	 *  
	* @param record                            Record to be updated
	* @param result                            Record as it already exists in the database
	* @param conn                              Database connection initiated in the upload method.
	* @param dbname                            Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	 */
	public static void updateRecord(CSVRecord record, ResultSet result,  Connection conn, String dbname) {
		String id = "";
		try {
			id = result.getString("ID");
		}
		catch(Exception e) {;}
		String updateQuery = "Update   "+dbname+".project SET PROJECT_TITLE= ?, source_url=?, "
				+ "PROJECT_START_DATE=?, PROJECT_END_DATE=?, PROJECT_TYPE=?, "
				+ "ACTIVITY_STATUS=?, DATE_ENTERED=?,  "
				+ "archive=?,LAST_UPDATE=?,  LAST_UPDATE_BY=? where ID = "
				+ "?;";
		ResultSet dummy = null;
		String PROJECT_NUMBER = recordGet("project__PROJECT_NUMBER", record,dummy);
		String PROJECT_TITLE = null;
		String source_url = "";
		String PROJECT_START_DATE = "";
		String PROJECT_END_DATE = "";	
		String PROJECT_TYPE = "";
		String  ACTIVITY_STATUS = "";
		String  DATE_ENTERED = null;	
		String  archive = "1";
		String  LAST_UPDATE = null;	
		String  LAST_UPDATE_BY = "air";
	
		/** 
		 * If a newer value is found in CSV, that value is used.
		 * If no new value is found, the value from the existing record in the database is used.
		 */
		PROJECT_TITLE = recordGet("project__PROJECT_TITLE", record, result);
		PROJECT_START_DATE = recordGet("project__PROJECT_START_DATE", record, result);
		PROJECT_END_DATE = recordGet("project__PROJECT_END_DATE",record, result);
		PROJECT_TYPE = recordGet("project__PROJECT_TYPE", record, result);
		ACTIVITY_STATUS = recordGet("project__ACTIVITY_STATUS", record, result);
		DATE_ENTERED = currentStamp;
		archive = recordGet("project__archive", record, result);
		LAST_UPDATE = currentStamp;
		LAST_UPDATE_BY = recordGet("project__LAST_UPDATE_BY", record, result);
	
	
		if (PROJECT_START_DATE!=null) if (PROJECT_START_DATE.trim().equals("")) PROJECT_START_DATE = null;
		if (PROJECT_END_DATE!=null) if (PROJECT_END_DATE.trim().equals("")) PROJECT_END_DATE = null;
		if (PROJECT_TYPE!=null) if (PROJECT_TYPE.trim().equals("")) PROJECT_TYPE = null;
		if (ACTIVITY_STATUS!=null) if (ACTIVITY_STATUS.trim().equals("")) ACTIVITY_STATUS = null;
		if (archive!=null) if (archive.trim().equals("")) archive = "1";

		String [] arr = {PROJECT_TITLE, source_url, PROJECT_START_DATE, PROJECT_END_DATE, 
				PROJECT_TYPE, ACTIVITY_STATUS, DATE_ENTERED, archive, LAST_UPDATE, LAST_UPDATE_BY, id};
		MysqlConnect.uploadSQL(arr, conn, updateQuery);
	}
	
	/**
	 * This method reads in the institution name from the CSV file, and tries to find it in database. If not, the ID is returned. If not, a new record is added and the new ID is returned.
	 *  
	* @param record                            Record from the CSV file, whose institute is to be found. 
	* @param conn                              Database connection initiated in the upload method.
	* @param dbname                            Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.

    * return                                   The ID for the institute
	 */
	public static String checkAddInst(CSVRecord record, Connection conn, String dbname) {
		String institution_data__INSTITUTION_NAME = "";
		ResultSet dummy = null;
		try {
			institution_data__INSTITUTION_NAME = recordGet("institution_data__INSTITUTION_NAME", record,dummy);
		}
		catch (Exception e) {return "-1";}
		if (institution_data__INSTITUTION_NAME == "") return "-1";
		String finalID = "";
		/**
		 * Let's make sure this institution definitely doesn't exist:
		 */
		
		/**
		 * Get all institutions to apply a matching algorithm based on Jaro-Winkler distance: a bit of on-the-fly disambiguation here.
		 */
		ResultSet institutes = null;
		String GetInsts = "SELECT ID, INSTITUTION_NAME FROM   "+dbname+".institution_data;";
		String a[] = {};
		
		institutes = MysqlConnect.uploadSQLResult(a, conn, GetInsts);
		
		double finalRatio = 1;

		String finalInst = "";

		try {
			while(institutes.next()) {
				String inst = institutes.getString("INSTITUTION_NAME");
				/**
				 * We will only look at institutes that have at least three letters in common with our institute
				 * This will speed up matching
				*/
				char[] s1Array = inst.toCharArray();
				char [] s2Array = institution_data__INSTITUTION_NAME.toCharArray();
				Set<Character>s1CharSet = new HashSet<Character>();
				Set<Character>s2CharSet = new HashSet<Character>(); 
				for(char c:s1Array){
					s1CharSet.add(c);
				}
				for(char c: s2Array){
					s2CharSet.add(c);
				}
				s1CharSet.retainAll(s2CharSet);
				int commonCount = s1CharSet.size();
				if(commonCount<3) continue;
				/**
				 *  Now that we know that there are at least three characters in common, lets see if
				 *   institution_data__INSTITUTION_NAME is a substring of the other institute to account for short names
				 */
				if (commonCount == institution_data__INSTITUTION_NAME.length()) {
					finalID = institutes.getString("ID");
					finalInst = inst;
					break;
				}
				/**
				 * Lets calculate the levenshtein distance for the rest to come up with the best match:
				 */
				float len  = (institution_data__INSTITUTION_NAME.length() > inst.length()) ? institution_data__INSTITUTION_NAME.length() : inst.length();
				double ratio =  StringUtils.getLevenshteinDistance(institution_data__INSTITUTION_NAME.toLowerCase(), inst.toLowerCase())/len;
				if (ratio < finalRatio) {
					finalID = institutes.getString("ID");
					finalInst = inst;
					finalRatio = ratio;
				}

			}
			if (finalRatio < 0.25 ) {
				return finalID;
			}
		}
		catch (Exception e) {
			;
		}
		/**
		 * Insert new record for the institute
		 */
		String INSTITUTION_NAME = "";
		String INSTITUTION_DEPARTMENT = "";
		String INSTITUTION_ADDRESS1 = "";
		String INSTITUTION_ADDRESS2 = "";
		String INSTITUTION_CITY = "";
		String INSTITUTION_STATE = "";
		String INSTITUTION_COUNTRY = "";
		String INSTITUTION_ZIP = "";
		String COMMENTS = "";
		String DATE_ENTERED = currentStamp;
		String INSTITUTION_URL = "";

		INSTITUTION_DEPARTMENT = recordGet("institution_data__INSTITUTION_DEPARTMENT", record,dummy);
		INSTITUTION_ADDRESS1 = recordGet("institution_data__INSTITUTION_ADDRESS1", record,dummy);
		INSTITUTION_ADDRESS2 = recordGet("institution_data__INSTITUTION_ADDRESS2", record,dummy);
		INSTITUTION_CITY = recordGet("institution_data__INSTITUTION_CITY", record,dummy);
		INSTITUTION_STATE = recordGet("institution_data__INSTITUTION_STATE", record,dummy);
		try {
				int stateID = Integer.parseInt(INSTITUTION_STATE);
		}
		catch (Exception e) {
		INSTITUTION_STATE = "";			}
		COMMENTS = recordGet("COMMENTS", record,dummy);
		INSTITUTION_URL = recordGet("institution_data__INSTITUTION_URL", record,dummy);
		INSTITUTION_COUNTRY = recordGet("institution_data__INSTITUTION_COUNTRY", record,dummy);
		if(!INSTITUTION_ADDRESS1.equals("")) INSTITUTION_ADDRESS1 = Junidecode.unidecode(INSTITUTION_ADDRESS1);
			
		String insertQuery = "INSERT INTO  "+dbname+".institution_data (INSTITUTION_NAME, INSTITUTION_DEPARTMENT, INSTITUTION_ADDRESS1, "
				+ "INSTITUTION_ADDRESS2, INSTITUTION_CITY, INSTITUTION_COUNTRY, INSTITUTION_STATE, "
				+ "INSTITUTION_ZIP, DATE_ENTERED, COMMENTS, INSTITUTION_URL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		if (INSTITUTION_COUNTRY.equals("")) INSTITUTION_COUNTRY = "0";
		if (INSTITUTION_STATE.equals("")) INSTITUTION_STATE = "0";

		String arr[] = {INSTITUTION_NAME, INSTITUTION_DEPARTMENT, INSTITUTION_ADDRESS1, INSTITUTION_ADDRESS2, 
				INSTITUTION_CITY, INSTITUTION_COUNTRY, INSTITUTION_STATE, INSTITUTION_ZIP, DATE_ENTERED, COMMENTS,
				INSTITUTION_URL};
		MysqlConnect.uploadSQL(arr, conn, insertQuery);
		
		String getID = " SELECT ID FROM  "+dbname+".institution_data WHERE INSTITUTION_NAME = ?;";
		ResultSet ID =null;
		String instname = recordGet("institution_data__INSTITUTION_NAME", record,dummy);
		if (!instname.equals("")) instname = Junidecode.unidecode(instname);
		String arr1[] = {instname};
		ID = MysqlConnect.uploadSQLResult(arr1, conn, getID);
		try {
			ID.next();
			finalID = ID.getString(1);
		}
		catch (Exception e) {
			;
		}

		return finalID;
	}


	/**
	 * This method reads in the PI name from the CSV file, and tries to find it in database. If found, the ID is returned. If not, a new record is added and the new ID is returned.
	 *  
	* @param record                            Record from the CSV file, whose institute is to be found. 
	* @param institution_index__inst_id        The institution that the PI belongs to 
	* @param conn                              Database connection initiated in the mainUpload method.
	* @param dbname                            Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.

    * return                                   The ID for the PI
	 */


	public static String getPIid(CSVRecord record, String institution_index__inst_id, Connection conn, String dbname) {
		String investigator_data__name = "";
		ResultSet dummy = null;
		try {
			investigator_data__name = recordGet("investigator_data__name", record,dummy);
		}
		catch(Exception e) {;}
		if (investigator_data__name.equals("")) return "-1";
		String query = "";
		/**
		 * Some data sources do not have any institution information - so, we just want to make sure that in those cases we still upload PI info.
		 */
		if (!institution_index__inst_id.equalsIgnoreCase("-1")) {
			query = "SELECT ID, name FROM   "+dbname+".investigator_data where INSTITUTION = ?;";
			
		} else {
			query = "SELECT ID, name FROM   "+dbname+".investigator_data where INSTITUTION != ?;";
		}
			
		

		ResultSet pis = null;
		String arr[] = {institution_index__inst_id};
		MysqlConnect.uploadSQLResult(arr, conn, query);

		String finalID = "";
		double finalRatio = 3;

		String finalPI = "";
		try {
			while(pis.next()) {
				
				String piname = pis.getString("name");
				/**
				 * calculate the levenshtein distance for the rest to come up with the best match
				 */
				
				float len  = (investigator_data__name.length() > piname.length()) ? investigator_data__name.length() : piname.length();
				double ratio =  StringUtils.getLevenshteinDistance(investigator_data__name.toLowerCase(), piname.toLowerCase());
				if (ratio < finalRatio) {
					finalID = pis.getString("ID");
					finalPI = piname;
					finalRatio = ratio;
				}
			}
			if (finalRatio <= 2 ) {
				return finalID;
			}
		}
		catch (Exception e) {
			;
		}

		/**
		 * Insert new record for the PI:
		 */
		String name = investigator_data__name;
		String EMAIL_ADDRESS = "";
		String PHONE_NUMBER = "";
		String INSTITUTION = institution_index__inst_id;
		if (INSTITUTION.equalsIgnoreCase("")) INSTITUTION=null; 
		String DATE_ENTERED = currentStamp;

		
		EMAIL_ADDRESS = recordGet("investigator_data__EMAIL_ADDRESS", record,dummy);
		PHONE_NUMBER = recordGet("investigator_data__PHONE_NUMBER", record,dummy);
		if(!name.equals("")) name = Junidecode.unidecode(name);
		String insertQuery = "INSERT INTO  "+dbname+".investigator_data (name, EMAIL_ADDRESS, PHONE_NUMBER, "
				+ "INSTITUTION, DATE_ENTERED) VALUES (?, ?, ?, ?, ?);";
		String arr1[] = {name, EMAIL_ADDRESS, PHONE_NUMBER, INSTITUTION, DATE_ENTERED};
		MysqlConnect.uploadSQL(arr1, conn, insertQuery);

		String getID = "SELECT ID FROM  "+dbname+".investigator_data WHERE name = ? and INSTITUTION = ?;";
		if(!name.equals("")) name = Junidecode.unidecode(name);
		String arr2[] = {name, INSTITUTION};
		ResultSet ID =null;
		
		ID = MysqlConnect.uploadSQLResult(arr2, conn, getID);

		try {
			ID.next();
			finalID = ID.getString(1);
		}
		catch (Exception e) {
			;
		}
		return finalID;
	}
	
	
	/**
	 * This method returns the value of column x in the CSVRecord. If x is not a column in the CSV,
	 * it looks for x in the ResultSet. If not found there, an empty string is returned
	 
	 * @param x                                 Value to be looked up in the CSVRecord
	 * @param record                            Record from the CSV file,  where x is to be found
	 * @param result                            Record from the ResultSet,  where x is to be found

     * @return                                  The value of x found in CSVRecord or ResultSet
	 */
	public static String recordGet(String x, CSVRecord record,  ResultSet result) {
		String y = "";
		try {
			y = record.get(x);
		}
		catch(Exception e) { 
			try {
			y = result.getString(x);
			}
			catch(Exception e1) {;}
		}
		return y;
	}
}
