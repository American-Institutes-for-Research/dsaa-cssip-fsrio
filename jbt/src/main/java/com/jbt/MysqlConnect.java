package com.jbt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class has a number of reusable methods to query the MySQL FSRIO database and check whether any projects that are currently being scraped already exist there. 
 */

public class MysqlConnect {
	public static Connection connection(String host, String user, String passwd) {
		Connection conn = null;
        try {
            // The newInstance() call is a work around for some
            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            
            try {
            	
                conn =
                   DriverManager.getConnection("jdbc:mysql://"+host+"?" +
                                               "user="+user+"&password="+passwd);
 
            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQL Exception: Please contact NAL IT to handle this error. This means the Java database connection driver has not started. Details can be found below.");
                System.out.println("SQLException: " + ex.getMessage());
                System.exit(0);
        }
        }
        catch (Exception ex) {
            // handle the error
        }
        return conn;
    }	

    public static void closeConnection(Connection conn) {
        try {
            conn.close();    
        } catch (SQLException ex) {
            System.out.println("SQL Exception: Please contact NAL IT to handle this error. This means the Java database connection driver has not started. Details can be found below.");
            System.out.println("SQLException: " + ex.getMessage());
            System.exit(0);
        }
        
    }
    
    /**
	* Check if investigator exists in the current FSRIO Research Projects Database. By definition, the unique pair is investigator-institution - so, we are checking for both here if they exist.
	* The last Exception except here needs to be ignored because by default we assume that the PI does not exist in the database and assign -1 index.
	* If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	* 
	* @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	* @param	investigator_index__inv_id	Principal investigator ID that exists in the investigator_data and investigator_index tables of the FSRIO Research Projects Database. By default, it is -1 if the name does not exist in the FSRIO DB.
	* @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	* @param	investigator_data__name	Full investigator name on individual projects in the format of FSRIO Research Projects Database: [Last Name], [First Name].
	* @param	institution_index__inst_id	Institution ID that exists in the institution_data and institution_index tables of the FSRIO Research Projects Database. By default, it is -1 if the institution name does not exist in the FSRIO DB. Some data sources do not have any institution-related information.
	* @return	investigator_index__inv_id	By default, it will be -1 in case the investigator name does not exist in the current FSRIO Database. It will update accordingly if MySQL finds within the DB.
	*/
    public static Integer GetInvestigatorSQL(String dbname, Integer investigator_index__inv_id, Connection conn, String investigator_data__name, int institution_index__inst_id) {
    	String GetInvestigatorSQL = "";
    	if (institution_index__inst_id == -2) {
    		GetInvestigatorSQL = "SELECT ID FROM "+dbname+".investigator_data WHERE NAME LIKE ?;";	
    	} else {
    		GetInvestigatorSQL = "SELECT ID FROM "+dbname+".investigator_data WHERE NAME LIKE ? AND INSTITUTION = ?;";
    	}
    	ResultSet result = null;
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
			preparedStmt.setString(1, investigator_data__name);
			if (institution_index__inst_id != -2) {
				preparedStmt.setString(2, String.valueOf(institution_index__inst_id));
			}
			result = preparedStmt.executeQuery();
			result.next();
			investigator_index__inv_id = result.getInt(1);
		}
		catch (Exception e) {;}
		return investigator_index__inv_id;
    }

    /**
	* Check if institution data exists in the current FSRIO Research Projects Database.
	* The last Exception except here needs to be ignored because by default we assume that the PI does not exist in the database and assign -1 index.
	* If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	* 
	* @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	* @param	institution_index__inst_id	Institution ID that exists in the institution_data and institution_index tables of the FSRIO Research Projects Database. By default, it is -1 if the institution name does not exist in the FSRIO DB. Some data sources do not have any institution-related information.
	* @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	* @param	institution_data__INSTITUTION_NAME	Full institution name on individual projects.
	*
	* @return	institution_index__inst_id	By default, it will be -1 in case the institution name does not exist in the current FSRIO Database. It will update accordingly if MySQL finds within the DB.
	*/
    public static Integer GetInstitutionSQL(String dbname, Integer institution_index__inst_id, Connection conn, String institution_data__INSTITUTION_NAME) {
    
	    /** 
		* Check if institution exists in DB
		* By default we assume that it does not exist in the DB. 
		*/
		String GetInstIDsql = "SELECT ID FROM  "+dbname+".institution_data WHERE INSTITUTION_NAME LIKE ?;";
		ResultSet rs = null;
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
			preparedStmt.setString(1, institution_data__INSTITUTION_NAME); 
			rs = preparedStmt.executeQuery();
	
			rs.next();
			institution_index__inst_id = rs.getInt(1);
		}
		catch (Exception except) {
		}
		return institution_index__inst_id;
    }
    
    
    /**
	* Check if country index can be retrieved from the current FSRIO Research Projects Database given data from the webscraper.
	* The last Exception except here needs to be ignored because the comment is added within main scraper and by default it is empty string.
	* If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	* 
	* @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	* @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	* @param	institution_data__INSTITUTION_COUNTRY	Full institution country per institution name on individual projects.
	* 
	* @return institution_data__INSTITUTION_COUNTRY By default, it is empty string. The method should return an index of country from the lookup table in DB.
	*/
    public static String GetCountrySQL(String dbname, String institution_data__INSTITUTION_COUNTRY, Connection conn) {
    	/** 
		* Check the country index in DB.
		* By default we assume that it exists in the DB but there is possibility it is not spelled correctly or else. 
		*/
    	String GetCtrySQL = "SELECT * FROM  "+dbname+".countries WHERE COUNTRY_NAME = ?";
		ResultSet result = null;
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(GetCtrySQL);
			preparedStmt.setString(1, institution_data__INSTITUTION_COUNTRY);
			result = preparedStmt.executeQuery();
			result.next();
			institution_data__INSTITUTION_COUNTRY = result.getString(1);
		}
		catch (Exception except) {;}
	    
		return institution_data__INSTITUTION_COUNTRY;
    }
    
    /**
	* Check if state index can be retrieved from the current FSRIO Research Projects Database given data from the webscraper.
	* The last Exception except here needs to be ignored because the comment is added within main scraper and by default it is empty string.
	* If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	* 
	* @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	* @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	* @param	stateRegex	This is the state abbreviation that is retrieved in preceding processing within the webscraper calling for this method, particularly Efsa.
	*
	* @return state_OK By default, it is empty string. The method should return the string of state abbreviation from the lookup table in DB.
	*/
    public static String GetStateSQL(String dbname, Connection conn, String stateRegex) {
    	/** 
		* Check the state index in DB.
		* By default we assume that it exists in the DB but there is possibility it is not spelled correctly or else. 
		*/
		String state_OK = "";
    	try {
			String GetStSQL = "SELECT id,abbrv FROM  "+dbname+".states";
			PreparedStatement preparedStmt = conn.prepareStatement(GetStSQL);
			ResultSet result = preparedStmt.executeQuery();
			while (result.next()) {
				String stateID = result.getString(1);
				String state = result.getString(2);
				/**
				 * Looping through all state names to recognize if it matches the stateRegex.
				 */
				Pattern patState = Pattern.compile("("+state+")");
				Matcher matchState = patState.matcher(stateRegex);
				if (matchState.find()) {
					state_OK = stateID+"_"+state;
				}
				
			}
		}
		catch (Exception except) {;}
    	return state_OK;
    }
    
    /**
     * Check if agency index can be retrieved from the current FSRIO Research Projects Database.
     * This is only applicable to NIH because it has multiple institutes and centers.
	 * The last Exception except here needs to be ignored because the comment is added within main scraper and by default it is empty string.
     * If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	 * 
     * @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	 * @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	 * @param project__AGENCY_FULL_NAME Full name of the NIH institute/center as retrieved from the file.
     * @param agency_index__aid Agency ID if agency exists in the FSRIO Research Projects Database. The ID is numeric and assigned based on the agency name.
     * @return Agency ID if it was found in FSRIO DB. Otherwise, default -1.
     */
    public static Integer GetAgencySQL(String dbname, Connection conn, String project__AGENCY_FULL_NAME, int agency_index__aid) {
    
    	String GetAgencySQL = "SELECT ID FROM  "+dbname+".agency_data WHERE AGENCY_FULL_NAME LIKE ?;";
		ResultSet result = null;
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(GetAgencySQL);
			preparedStmt.setString(1, project__AGENCY_FULL_NAME);
			result = preparedStmt.executeQuery();
			result.next();
			agency_index__aid = Integer.parseInt(result.getString(1));
		}
		catch (Exception except) {;}
		return agency_index__aid;
    }
    
    /**
	* Check if project data exists in the current FSRIO Research Projects Database by the project number, project start date, and project end date. Institution and PI data should also be taken into account as checked above.
	* The Exception ex here needs to be ignored because by default we assume that the project data does not exist in the database.
	* If the exception is not ignored it will give multiple "Illegal operation on empty result set."
	* 
	* @param	dbname	FSRIO Research Projects Database name that is defined in config file (typically, process.cfg).
	* @param	project__PROJECT_NUMBER	This is empty by default: we are just checking with this method whether project data exists at all in the DB.
	* @param	conn	Database connection initiated once per scraper in the main[Subclass] method.
	* @param	project__PROJECT_START_DATE	The project start date as retrieved from the individual project pages. Sometimes there is no such data and therefore we also check if that can be NULL.	
	* @param	project__PROJECT_END_DATE	The project end date as retrieved from the individual project pages. Sometimes there is no such data and therefore we also check if that can be NULL.
	* @param	investigator_index__inv_id	Principal investigator ID that exists in the investigator_data and investigator_index tables of the FSRIO Research Projects Database. By default, it is -1 if the name does not exist in the FSRIO DB. Some data sources do not have any PI-related information.
	* @param	institution_index__inst_id	Institution ID that exists in the institution_data and institution_index tables of the FSRIO Research Projects Database. By default, it is -1 if the institution name does not exist in the FSRIO DB. Some data sources do not have any institution-related information.
	* @return	project__PROJECT_NUMBER	By default, the project data does not exist in the FSRIO DB. The project number can be alphanumeric string and will be returned here if exists. 
	*/
    public static String GetProjectNumberSQL(String dbname, String project__PROJECT_NUMBER, Connection conn, String project__PROJECT_START_DATE, String project__PROJECT_END_DATE, Integer investigator_index__inv_id, Integer institution_index__inst_id) {
        
	    /**
	     * There can be multiple cases where only investigator name or institution name is available per data source. 
	     * We need to handle all those situations and only check fields that exists within the DB with regard to such source.
	     * By default, we set in all scrapers investigator_index__inv_id and institution_index__inst_id == -2 in scrapers where we know those respective fields do not exist.
	     */
    	
    	String query = "";
    	/**
    	 * We need a special status string that will tell us if the project number was found in the DB or not so that we know whether to skip link or proceed. 
    	 */
    	String status = "";
    	if (investigator_index__inv_id == -2 && institution_index__inst_id == -2) {
	    	query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p "
					+ "where p.PROJECT_NUMBER = ? and p.PROJECT_START_DATE = ?"
					+ " and p.PROJECT_END_DATE = ?;";	
	    } else if (investigator_index__inv_id == -2) {
	    	query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p "
					+ "left outer join "+dbname+".institution_index insti on insti.pid = p.id "
					+ "where p.PROJECT_NUMBER = ? and p.PROJECT_START_DATE = ? "
					+ " and p.PROJECT_END_DATE = ? and insti.inst_id = ?;";
	    } else if (institution_index__inst_id == -2) {
	    	query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p "
					+ "left outer join "+dbname+".investigator_index ii on ii.pid = p.id "
					+ "where p.PROJECT_NUMBER = ? and p.PROJECT_START_DATE = ?"
					+ " and p.PROJECT_END_DATE = ? and ii.inv_id = ?;";
	    } else {
		    query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p "
					+ "left outer join "+dbname+".investigator_index ii on ii.pid = p.id "
					+ "left outer join "+dbname+".institution_index insti on insti.pid = p.id "
					+ "where p.PROJECT_NUMBER = ? and p.PROJECT_START_DATE = ?"
					+ " and p.PROJECT_END_DATE = ? and ii.inv_id = ? and insti.inst_id = ?;";
	    }
    	
		ResultSet result = null;
		try {
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString(1, project__PROJECT_NUMBER);
			preparedStmt.setString(2, project__PROJECT_START_DATE); 
			preparedStmt.setString(3, project__PROJECT_END_DATE); 
			if (investigator_index__inv_id == -2 && institution_index__inst_id == -2) {		    } 
			else if (investigator_index__inv_id == -2) {preparedStmt.setString(4, String.valueOf(institution_index__inst_id));} 
		    else if (institution_index__inst_id == -2) {preparedStmt.setString(4, String.valueOf(investigator_index__inv_id));} 
		    else {
		    	preparedStmt.setString(4, String.valueOf(investigator_index__inv_id)); 
				preparedStmt.setString(5, String.valueOf(institution_index__inst_id));
		    }
			result = preparedStmt.executeQuery();
			result.next();
			String number = result.getString(1);
			status = "Found";
		}
		catch (Exception ex) { status = "Miss";}
		return status;
    }

	
}
