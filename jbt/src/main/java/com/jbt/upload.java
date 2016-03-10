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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import net.sf.junidecode.Junidecode;

public class upload {
	static Date current = new Date();
	static DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
	static String currentStamp = dateFormatCurrent.format(current);

	public static void main(File Filename, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		System.out.println("Working on "+Filename);
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		uploadRecords(Filename, host, user, passwd, dbname);
	}

	public static void uploadRecords(File fileName, String host, String user, String passwd, String dbname) throws IOException {
		MultiMap PIS = new MultiValueMap();
		MultiMap INSTITUTIONS = new MultiValueMap();

		Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().withDelimiter('\t').parse(new FileReader(fileName));
	
		for (CSVRecord record : records) {
			String project__PROJECT_NUMBER = "";
			//question for Evgeny: Is the project number ever missing?
			try {
				project__PROJECT_NUMBER = record.get("project__PROJECT_NUMBER");
			}
			catch (Exception e) {
				;
			}
			String institution_index__inst_id = "";
			String investigator_index__inv_id= "";
			String investigator_data__name ="";
			String end = "";
			try {
				institution_index__inst_id = record.get("institution_index__inst_id");
			}
			catch(Exception e) {;}
			try {
				investigator_index__inv_id = record.get("investigator_index__inv_id");
			}
			catch(Exception e) {;}
			try {
				end = record.get("project__PROJECT_END_DATE");	
			
			}
			catch(Exception e) {;} 

			Connection conn = MysqlConnect.connection(host,user,passwd);

			// Check which csv file we are parsing, and if it is something specific, the institutions are not required. So modify logic. 
			//AE: So I did this in the getPiID function. If there institution_name is missing, the query gets all PIs, if not then limits to institution.
			if (institution_index__inst_id.equals("-1"))  institution_index__inst_id = checkAddInst(record, host, user, passwd, dbname);
			investigator_index__inv_id = getPIid(record, institution_index__inst_id, host, user, passwd, dbname);
			
			//Project number	
			String query = "SELECT * FROM "+dbname+".project p left join "
					+ dbname+".institution_index inst on inst.pid =  p.id left join "
							+ dbname+".investigator_index inv on inv.pid = p.id where PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\" order by date_entered desc limit 1";
			ResultSet result = null;
			try{
				result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
			}
			catch(Exception e) {System.err.println(query);}
			try {
				result.next();
				String t = result.getString("ID");
				String inst = result.getString("inst_id");
				String investigator = result.getString("inv_id");
				String endDate = result.getString("PROJECT_END_DATE");
				
				if (inst == null) inst = ""; 
				if (investigator == null) investigator = "";
				if (endDate == null) endDate = "";
				if(t == null) t = "";
				// AT least one of these things should have real new value, before we update
				if (!t.isEmpty()) {
					if(!investigator_index__inv_id.equals("-1") || !institution_index__inst_id.equals("-1")) {
					if (!investigator.equalsIgnoreCase(investigator_index__inv_id) || !inst.equalsIgnoreCase(institution_index__inst_id) || !endDate.equalsIgnoreCase(end))  {
							// Add the new PI and Inst information to   a dictionary, but only if it is not -1. 
							if (!investigator_index__inv_id.equals("-1"))
								PIS.put(t, investigator_index__inv_id);
							if (!institution_index__inst_id.equals("-1"))
								INSTITUTIONS.put(t, institution_index__inst_id);
							updateRecord(record, result, host, user, passwd, dbname);
							}
					}
				}
	
				
				}
			catch (Exception ex) {
				insertRecord(record, institution_index__inst_id, investigator_index__inv_id, host, user, passwd, dbname);
			}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}
		}

		// Now we can update the index tables with all new PI and Institution information.  
		Connection conn = MysqlConnect.connection(host,user,passwd);
		// PI TABLE
		Set<String> keys = PIS.keySet();
		for(String s : keys) {
			String delete = "DELETE FROM  "+dbname +".investigator_index WHERE pid= " + s;
			ArrayList<String> list = (ArrayList<String>)(PIS.get(s));
			if(list == null) continue;
			if(list.isEmpty()) continue;
			Set<String> values = new HashSet<String>(list);  
			for (String s2: values) {
				String insertQuery = "INSERT INTO  "+dbname+".investigator_index (pid, inv_id)"
						+ " VALUES (?, ?);";
				try {
					PreparedStatement preparedStmt2 = conn.prepareStatement(insertQuery);
					preparedStmt2.setString(1, s);
					preparedStmt2.setString(2, s2);	
					preparedStmt2.execute();
					
				}
				catch (Exception e) {;}
				finally {
					if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
				}
			}

		}

		//INSTITUTION TABLE
		keys = INSTITUTIONS.keySet();
		for(String s : keys) {
			String delete = "DELETE FROM  "+dbname+".institution_index WHERE pid= " + s;
			ArrayList<String> list = (ArrayList<String>)(PIS.get(s));
			if(list == null) continue;
			if(list.isEmpty()) continue;
			Set<String> values = new HashSet<String>(list);  
			for (String s2: values) {
				String insertQuery = "INSERT INTO  "+dbname+".institution_index (pid, inv_id)"
						+ " VALUES (?, ?);";
				try {
					PreparedStatement preparedStmt2 = conn.prepareStatement(insertQuery);
					preparedStmt2.setString(1, s);
					preparedStmt2.setString(2, s2);	
					preparedStmt2.execute();
					
				}
				catch (Exception e) {;}
				finally {
					if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
				}
			}

		}
		try {
			conn.close();
		}
		catch(Exception e) {;}
	}

	public static void insertRecord(CSVRecord record, String institution_index__inst_id, String investigator_index__inv_id, String host, String user, String passwd, String dbname) {
		Connection conn = MysqlConnect.connection(host,user,passwd);
		String insertQuery = "INSERT INTO  "+dbname+".project (PROJECT_NUMBER, PROJECT_TITLE, source_url, "
				+ "PROJECT_START_DATE, PROJECT_END_DATE, PROJECT_FUNDING, PROJECT_TYPE, "
				+ "PROJECT_KEYWORDS, PROJECT_IDENTIFIERS, PROJECT_COOPORATORS, PROJECT_ABSTRACT, "
				+ "PROJECT_PUBLICATIONS, other_publications, PROJECT_MORE_INFO, PROJECT_OBJECTIVE,"
				+ "PROJECT_ACCESSION_NUMBER, ACTIVITY_STATUS, DATE_ENTERED, COMMENTS, AUTO_INDEX_QA,"
				+ "archive,LAST_UPDATE,  LAST_UPDATE_BY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
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
		String  AUTO_INDEX_QA = "";
		String  archive = "1";
		String  LAST_UPDATE = null;	
		String  LAST_UPDATE_BY = "air";
		String PROJECT_OBJECTIVE= "";
		try {
			PROJECT_NUMBER = record.get("project__PROJECT_NUMBER");
		}
		catch (Exception e) {;}
		try {
			PROJECT_TITLE = record.get("project__PROJECT_TITLE");
		}
		catch (Exception e) {;}

		try {
			source_url = record.get("project__source_url");
		}
		catch (Exception e) {;}


		try {
			PROJECT_START_DATE = record.get("project__PROJECT_START_DATE");
		}
		catch (Exception e) {;}


		try {
			PROJECT_END_DATE = record.get("project__PROJECT_END_DATE");
		}
		catch (Exception e) {;}


		try {
			PROJECT_FUNDING = record.get("project__PROJECT_FUNDING");
		}
		catch (Exception e) {;}


		try {
			PROJECT_TYPE = record.get("project__PROJECT_TYPE");
		}
		catch (Exception e) {;}


		try {
			PROJECT_KEYWORDS = record.get("project__PROJECT_KEYWORDS");
		}
		catch (Exception e) {;}


		try {
			PROJECT_IDENTIFIERS = record.get("project__PROJECT_IDENTIFIERS");
		}
		catch (Exception e) {;}


		try {
			PROJECT_COOPORATORS = record.get("project__PROJECT_COOPORATORS");
		}
		catch (Exception e) {;}


		try {
			PROJECT_ABSTRACT = record.get("project__PROJECT_ABSTRACT");
		}
		catch (Exception e) {;}


		try {
			PROJECT_PUBLICATIONS = record.get("project__PROJECT_PUBLICATIONS");
		}
		catch (Exception e) {;}


		try {
			other_publications = record.get("project__other_publications");
		}
		catch (Exception e) {;}


		try {
			PROJECT_MORE_INFO = record.get("project__PROJECT_MORE_INFO");
		}
		catch (Exception e) {;}


		try {
			PROJECT_ACCESSION_NUMBER = record.get("project__PROJECT_ACCESSION_NUMBER");
		}
		catch (Exception e) {;}
		try {
			PROJECT_OBJECTIVE = record.get("project__PROJECT_OBJECTIVE");
		}
		catch (Exception e) {;}

		try {
			ACTIVITY_STATUS = record.get("project__ACTIVITY_STATUS");
		}
		catch (Exception e) {;}


		try {
			DATE_ENTERED = currentStamp;
		}
		catch (Exception e) {;}

		try {
			COMMENTS = record.get("project__COMMENTS");
		}
		catch (Exception e) {;}

		try {
			AUTO_INDEX_QA = record.get("project__AUTO_INDEX_QA");
		}
		catch (Exception e) {;}


		try {
			archive = record.get("project__archive");
		}
		catch (Exception e) {;}


		try {
			LAST_UPDATE = currentStamp;
		}
		catch (Exception e) {;}

		try {
			LAST_UPDATE_BY = record.get("project__LAST_UPDATE_BY");
		}
		catch (Exception e) {;}
		if (PROJECT_START_DATE.trim().equals("")) PROJECT_START_DATE = null;
		if (PROJECT_END_DATE.trim().equals("")) PROJECT_END_DATE = null;
		if (PROJECT_FUNDING.trim().equals("")) PROJECT_FUNDING = null;
		if (PROJECT_TYPE.trim().equals("")) PROJECT_TYPE = null;
		if (ACTIVITY_STATUS.trim().equals("")) ACTIVITY_STATUS = null;
		if (archive.trim().equals("")) archive = "1";
		
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(insertQuery);
			preparedStmt.setString(1, PROJECT_NUMBER);
			preparedStmt.setString(2, Junidecode.unidecode(PROJECT_TITLE));
			preparedStmt.setString(3, source_url);
			preparedStmt.setString(4, PROJECT_START_DATE);
			preparedStmt.setString(5, PROJECT_END_DATE);	
			preparedStmt.setString(6, PROJECT_FUNDING);
			preparedStmt.setString(7, PROJECT_TYPE);
			preparedStmt.setString(8, PROJECT_KEYWORDS);
			preparedStmt.setString(9, PROJECT_IDENTIFIERS);
			preparedStmt.setString(10, PROJECT_COOPORATORS);	
			preparedStmt.setString(11, Junidecode.unidecode(PROJECT_ABSTRACT));	
			preparedStmt.setString(12, PROJECT_PUBLICATIONS);	
			preparedStmt.setString(13, other_publications);
			preparedStmt.setString(14, PROJECT_MORE_INFO);
			preparedStmt.setString(15,  Junidecode.unidecode(PROJECT_OBJECTIVE));
			preparedStmt.setString(16, PROJECT_ACCESSION_NUMBER);
			preparedStmt.setString(17, ACTIVITY_STATUS);
			preparedStmt.setString(18, DATE_ENTERED);	
			preparedStmt.setString(19, COMMENTS);
			preparedStmt.setString(20, AUTO_INDEX_QA);
			preparedStmt.setString(21, archive);
			preparedStmt.setString(22, LAST_UPDATE);	
			preparedStmt.setString(23, LAST_UPDATE_BY);
			preparedStmt.execute();
			
		}
		catch (Exception e) {System.err.println(e);}		
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}
		// Insert the new project into investigator_index
		
		
		
		conn = MysqlConnect.connection(host,user,passwd);
		// Get the ID for the project just inserted
		String query = "SELECT ID from "+dbname+".project order by ID desc limit 1";
		ResultSet result = null;
		String ID = "";
		try{
			result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
		}
		catch(Exception e) {System.err.println(query);}
		

		try {
			result.next();
			ID = result.getString("ID");
		}
		catch(Exception e) {;}		
		
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}

		
		if (!investigator_index__inv_id.equals("-1")) {
			conn = MysqlConnect.connection(host, user, passwd);
			insertQuery = "INSERT INTO "+dbname+".investigator_index (pid, inv_id)"
					+ " VALUES (?, ?);";
			try {
				PreparedStatement preparedStmt2 = conn.prepareStatement(insertQuery);
				preparedStmt2.setString(1, ID);
				preparedStmt2.setString(2, investigator_index__inv_id);
				preparedStmt2.execute();
			}
			catch (Exception e) {;}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}
		}
		if (!institution_index__inst_id.equals("-1")) {
			// Insert the new project into institution_index
			conn = MysqlConnect.connection(host,user,passwd);
			insertQuery = "INSERT INTO " + dbname + ".institution_index (pid, inst_id)"
					+ " VALUES (?, ?);";
			try {
				PreparedStatement preparedStmt2 = conn.prepareStatement(insertQuery);
				preparedStmt2.setString(1, ID);
				preparedStmt2.setString(2, institution_index__inst_id);	
				preparedStmt2.execute();

			}
			catch (Exception e) {;}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}

		}


				
	}

	public static void updateRecord(CSVRecord record, ResultSet result,  String host, String user, String passwd, String dbname) {
		Connection conn = MysqlConnect.connection(host,user,passwd);
		String id = "";
		try {
			id = result.getString("ID");
		}
		catch(Exception e) {;}
		String updateQuery = "Update  "+dbname+".project SET PROJECT_TITLE= ?, source_url=?, "
				+ "PROJECT_START_DATE=?, PROJECT_END_DATE=?, PROJECT_FUNDING=?, PROJECT_TYPE=?, "
				+ "PROJECT_KEYWORDS =?, PROJECT_IDENTIFIERS=?, PROJECT_COOPORATORS=?, PROJECT_ABSTRACT=?, "
				+ "PROJECT_PUBLICATIONS=?, other_publications=?, PROJECT_MORE_INFO=?, PROJECT_OBJECTIVE=?,"
				+ "PROJECT_ACCESSION_NUMBER=?, ACTIVITY_STATUS=?, DATE_ENTERED=?, COMMENTS=?, AUTO_INDEX_QA=?,"
				+ "archive=?,LAST_UPDATE=?,  LAST_UPDATE_BY=? where ID = "
				+ "\"" + id + "\";";
		String PROJECT_NUMBER = record.get("project__PROJECT_NUMBER");
		String PROJECT_TITLE = null;
		String source_url = "";
		String PROJECT_START_DATE = "";
		String PROJECT_END_DATE = "";	
		String PROJECT_FUNDING = "";
		String PROJECT_TYPE = "";
		String PROJECT_KEYWORDS = null;
		String PROJECT_IDENTIFIERS = null;
		String  PROJECT_COOPORATORS = null;	
		String  PROJECT_ABSTRACT = null;	
		String  PROJECT_PUBLICATIONS = null;	
		String  other_publications = "";
		String  PROJECT_MORE_INFO = null;
		String  PROJECT_ACCESSION_NUMBER = null;
		String  ACTIVITY_STATUS = "";
		String  DATE_ENTERED = null;	
		String  COMMENTS = null;
		String  AUTO_INDEX_QA = "";
		String  archive = "1";
		String  LAST_UPDATE = null;	
		String  LAST_UPDATE_BY = "air";
		String PROJECT_OBJECTIVE= null;
		try {
			try {
				PROJECT_TITLE = record.get("project__PROJECT_TITLE");
			}
			catch (Exception e) {PROJECT_TITLE = result.getString("PROJECT_TITLE");}

			try {
				PROJECT_START_DATE = record.get("project__PROJECT_START_DATE");
			}
			catch (Exception e) {PROJECT_START_DATE = result.getString("PROJECT_START_DATE");}


			try {
				PROJECT_END_DATE = record.get("project__PROJECT_END_DATE");
			}
			catch (Exception e) {PROJECT_END_DATE = result.getString("PROJECT_END_DATE");}


			try {
				PROJECT_FUNDING = record.get("project__PROJECT_FUNDING");
			}
			catch (Exception e) {PROJECT_FUNDING = result.getString("PROJECT_FUNDING");}


			try {
				PROJECT_TYPE = record.get("project__PROJECT_TYPE");
			}
			catch (Exception e) {PROJECT_TYPE = result.getString("PROJECT_TYPE");}


			try {
				PROJECT_KEYWORDS = record.get("project__PROJECT_KEYWORDS");
			}
			catch (Exception e) {PROJECT_KEYWORDS = result.getString("PROJECT_KEYWORDS");}


			try {
				PROJECT_IDENTIFIERS = record.get("project__PROJECT_IDENTIFIERS");
			}
			catch (Exception e) {PROJECT_IDENTIFIERS = result.getString("PROJECT_IDENTIFIERS");}


			try {
				PROJECT_COOPORATORS = record.get("project__PROJECT_COOPORATORS");
			}
			catch (Exception e) { PROJECT_COOPORATORS = result.getString("PROJECT_COOPORATORS");}


			try {
				PROJECT_ABSTRACT = record.get("project__PROJECT_ABSTRACT");
			}
			catch (Exception e) {PROJECT_ABSTRACT = result.getString("PROJECT_ABSTRACT");}


			try {
				PROJECT_PUBLICATIONS = record.get("project__PROJECT_PUBLICATIONS");
			}
			catch (Exception e) {PROJECT_PUBLICATIONS = result.getString("PROJECT_PUBLICATIONS");}


			try {
				other_publications = record.get("project__other_publications");
			}
			catch (Exception e) {other_publications = result.getString("other_publications");}


			try {
				PROJECT_MORE_INFO = record.get("project__PROJECT_MORE_INFO");
			}
			catch (Exception e) {PROJECT_MORE_INFO = result.getString("PROJECT_MORE_INFO");}

			try {
				PROJECT_OBJECTIVE = record.get("project__PROJECT_OBJECTIVE");
			}
			catch (Exception e) {PROJECT_OBJECTIVE = result.getString("PROJECT_OBJECTIVES");}

			try {
				PROJECT_ACCESSION_NUMBER = record.get("project__PROJECT_ACCESSION_NUMBER");
			}
			catch (Exception e) {PROJECT_ACCESSION_NUMBER = result.getString("PROJECT_ACCESSION_NUMBER");}


			try {
				ACTIVITY_STATUS = record.get("project__ACTIVITY_STATUS");
			}
			catch (Exception e) {ACTIVITY_STATUS = result.getString("ACTIVITY_STATUS");}


			try {
				DATE_ENTERED = currentStamp;
			}
			catch (Exception e) {;}

			try {
				COMMENTS = record.get("project__COMMENTS");
			}
			catch (Exception e) {COMMENTS = result.getString("COMMENTS");}

			try {
				AUTO_INDEX_QA = record.get("project__AUTO_INDEX_QA");
			}
			catch (Exception e) {AUTO_INDEX_QA = result.getString("AUTO_INDEX_QA");}


			try {
				archive = record.get("project__archive");
			}
			catch (Exception e) {archive = result.getString("archive");}


			try {
				LAST_UPDATE = currentStamp;
			}
			catch (Exception e) {;}

			try {
				LAST_UPDATE_BY = record.get("project__LAST_UPDATE_BY");
			}
			catch (Exception e) {LAST_UPDATE_BY = result.getString("LAST_UPDATE_BY");}

		}
		catch (Exception e) {;}
		if (PROJECT_START_DATE!=null) if (PROJECT_START_DATE.trim().equals("")) PROJECT_START_DATE = null;
		if (PROJECT_END_DATE!=null) if (PROJECT_END_DATE.trim().equals("")) PROJECT_END_DATE = null;
		if (PROJECT_FUNDING!=null) if (PROJECT_FUNDING.trim().equals("")) PROJECT_FUNDING = null;
		if (PROJECT_TYPE!=null) if (PROJECT_TYPE.trim().equals("")) PROJECT_TYPE = null;
		if (ACTIVITY_STATUS!=null) if (ACTIVITY_STATUS.trim().equals("")) ACTIVITY_STATUS = null;
		if (archive!=null) if (archive.trim().equals("")) archive = "1";

		try {
			PreparedStatement preparedStmt = conn.prepareStatement(updateQuery);
			preparedStmt.setString(1, PROJECT_TITLE);
			preparedStmt.setString(2, source_url);
			preparedStmt.setString(3, PROJECT_START_DATE);
			preparedStmt.setString(4, PROJECT_END_DATE);	
			preparedStmt.setString(5, PROJECT_FUNDING);
			preparedStmt.setString(6, PROJECT_TYPE);
			preparedStmt.setString(7, PROJECT_KEYWORDS);
			preparedStmt.setString(8, PROJECT_IDENTIFIERS);
			preparedStmt.setString(9, PROJECT_COOPORATORS);	
			preparedStmt.setString(10, PROJECT_ABSTRACT);	
			preparedStmt.setString(11, PROJECT_PUBLICATIONS);	
			preparedStmt.setString(12, other_publications);
			preparedStmt.setString(13, PROJECT_MORE_INFO);
			preparedStmt.setString(14, PROJECT_OBJECTIVE);
			preparedStmt.setString(15, PROJECT_ACCESSION_NUMBER);
			preparedStmt.setString(16, ACTIVITY_STATUS);
			preparedStmt.setString(17, DATE_ENTERED);	
			preparedStmt.setString(18, COMMENTS);
			preparedStmt.setString(19, AUTO_INDEX_QA);
			preparedStmt.setString(20, archive);
			preparedStmt.setString(21, LAST_UPDATE);	
			preparedStmt.setString(22, LAST_UPDATE_BY);

			preparedStmt.execute();
		}
		catch (Exception e) {;}		
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}
		try {
			conn.close();
		}
		catch(Exception e) {;}
	}

	public static String checkAddInst(CSVRecord record, String host, String user, String passwd, String dbname) {
		String institution_data__INSTITUTION_NAME = "";
		try {
			institution_data__INSTITUTION_NAME = record.get("institution_data__INSTITUTION_NAME");
		}
		catch (Exception e) {return "-1";}
		if (institution_data__INSTITUTION_NAME == "") return "-1";
		Connection conn = MysqlConnect.connection(host, user, passwd);
		String finalID = "";
		// Let's make sure this institution definitely doesn't exist:
		// Get all institutions
		ResultSet institutes = null;
		String GetInsts = "SELECT ID, INSTITUTION_NAME FROM  "+dbname+".institution_data";
		try {
			institutes = MysqlConnect.sqlQuery(GetInsts, conn, host,user,passwd);

		}
		catch (Exception e) {System.err.println(GetInsts);}
		double finalRatio = 1;

		String finalInst = "";

		try {
			while(institutes.next()) {
				String inst = institutes.getString("INSTITUTION_NAME");
				//We will only look at institutes that have at least three letters in common with our institute
				//This will speed up matching
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
				// Now that we know that there are at least three characters in common, lets see if
				// institution_data__INSTITUTION_NAME is a substring of the other institute to account for short names
				if (commonCount == institution_data__INSTITUTION_NAME.length()) {
					finalID = institutes.getString("ID");
					finalInst = inst;
					break;
				}
				// Lets calculate the levenshtein distance for the rest to come up with the best match:
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
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}
		conn = MysqlConnect.connection(host, user, passwd);
		// Insert new record for the institute:
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


		try {
			INSTITUTION_DEPARTMENT = record.get("institution_data__INSTITUTION_DEPARTMENT");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_ADDRESS1 = record.get("institution_data__INSTITUTION_ADDRESS1");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_ADDRESS2 = record.get("institution_data__INSTITUTION_ADDRESS2");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_CITY = record.get("institution_data__INSTITUTION_CITY");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_STATE = record.get("institution_data__INSTITUTION_STATE");
			try {
				int stateID = Integer.parseInt(INSTITUTION_STATE);
			}
			catch (Exception e) {
				INSTITUTION_STATE = "";			}
		}
		catch(Exception e) {;}

		try {
			COMMENTS = record.get("COMMENTS");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_URL = record.get("institution_data__INSTITUTION_URL");
		}
		catch(Exception e) {;}

		try {
			INSTITUTION_COUNTRY = record.get("institution_data__INSTITUTION_COUNTRY");
		}
		catch(Exception e) {;}

		String insertQuery = "INSERT INTO institution_data (INSTITUTION_NAME, INSTITUTION_DEPARTMENT, INSTITUTION_ADDRESS1, "
				+ "INSTITUTION_ADDRESS2, INSTITUTION_CITY, INSTITUTION_COUNTRY, INSTITUTION_STATE, "
				+ "INSTITUTION_ZIP, DATE_ENTERED, COMMENTS, INSTITUTION_URL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(insertQuery);
			preparedStmt.setString(1, INSTITUTION_NAME);
			preparedStmt.setString(2, INSTITUTION_DEPARTMENT);
			preparedStmt.setString(3, INSTITUTION_ADDRESS1);
			preparedStmt.setString(4, INSTITUTION_ADDRESS2);
			preparedStmt.setString(5, INSTITUTION_CITY);	
			preparedStmt.setString(6, INSTITUTION_COUNTRY);
			preparedStmt.setString(7, INSTITUTION_STATE);
			preparedStmt.setString(8, INSTITUTION_ZIP);
			preparedStmt.setString(9, DATE_ENTERED);
			preparedStmt.setString(10, COMMENTS);	
			preparedStmt.setString(11, INSTITUTION_URL);	

			preparedStmt.execute();
		}
		catch (Exception e) {;}		
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}

		conn = MysqlConnect.connection(host, user, passwd);
		String getID = " SELECT ID FROM "+dbname+".institution_data WHERE INSTITUTION_NAME = \"" +  record.get("institution_data__INSTITUTION_NAME") + "\";";
		ResultSet ID =null;
				
		try {
			ID = MysqlConnect.sqlQuery(getID, conn, host,user,passwd);
		}
		catch(Exception e) {
			System.err.println(getID);
		}
		try {
			ID.next();
			finalID = ID.getString(1);
		}
		catch (Exception e) {
			;
		}
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}
		return finalID;
	}





	public static String getPIid(CSVRecord record, String institution_index__inst_id, String host, String user, String passwd, String dbname) {
		String investigator_data__name = "";
		try {
			investigator_data__name = record.get("investigator_data__name");
		}
		catch(Exception e) {;}
		if (investigator_data__name.equals("")) return "-1";
		String query = "";
		if (!institution_index__inst_id.equalsIgnoreCase("-1"))
			query = "SELECT ID, name FROM  "+dbname+".investigator_data where INSTITUTION = \""+institution_index__inst_id+"\"";
		else 
			query = "SELECT ID, name FROM  "+dbname+".investigator_data;";
		Connection conn = MysqlConnect.connection(host,user,passwd);
		ResultSet pis = null;
		try {
			pis = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
		}
		catch(Exception e) {System.err.println(query);}
		String finalID = "";
		double finalRatio = 3;

		String finalPI = "";
		try {
			while(pis.next()) {
				
				String piname = pis.getString("name");
				// Lets calculate the levenshtein distance for the rest to come up with the best match:
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
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}

		// Insert new record for the PI:
		String name = investigator_data__name;
		String EMAIL_ADDRESS = "";
		String PHONE_NUMBER = "";
		String INSTITUTION = institution_index__inst_id;
		if (INSTITUTION.equalsIgnoreCase("")) INSTITUTION="0"; 
		String DATE_ENTERED = currentStamp;

		try {
			EMAIL_ADDRESS = record.get("investigator_data__EMAIL_ADDRESS");
		}
		catch(Exception e) {;}

		try {
			PHONE_NUMBER = record.get("investigator_data__PHONE_NUMBER");
		}
		catch(Exception e) {;}


		conn = MysqlConnect.connection(host,user,passwd);
		String insertQuery = "INSERT INTO  "+dbname+".investigator_data (name, EMAIL_ADDRESS, PHONE_NUMBER, "
				+ "INSTITUTION, DATE_ENTERED) VALUES (?, ?, ?, ?, ?);";
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(insertQuery);
			preparedStmt.setString(1, name);
			preparedStmt.setString(2, EMAIL_ADDRESS);
			preparedStmt.setString(3, PHONE_NUMBER);
			preparedStmt.setString(4, INSTITUTION);
			preparedStmt.setString(5, DATE_ENTERED);

			preparedStmt.execute();
			
		}
		catch (Exception e) {;}		
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}

		conn = MysqlConnect.connection(host,user,passwd);
		String getID = "SELECT ID FROM "+dbname+ ".investigator_data WHERE name = \"" +  name  
				+ "\" and INSTITUTION = " + INSTITUTION + ";";
		ResultSet ID =null;
		try {
			ID = MysqlConnect.sqlQuery(getID, conn, host,user,passwd);

		}
		catch (Exception e) {System.err.println(getID);}
		
		try {
			ID.next();
			finalID = ID.getString(1);
		}
		catch (Exception e) {
			;
		}
		finally {
			if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
		}
		return finalID;
	}
}
