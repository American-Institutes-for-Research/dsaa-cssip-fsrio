package com.jbt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 
            	//System.out.println(conn);
            } catch (SQLException ex) {
                // handle any errors
                //
                //System.out.println("SQLState: " + ex.getSQLState());
                //System.out.println("VendorError: " + ex.getErrorCode());
                System.out.println("SQL Exception: Please contact NAL IT to handle this error. This means the Java database connection driver has not started. Details can be found below.");
                System.out.println("SQLException: " + ex.getMessage());
        }
        }
        catch (Exception ex) {
            // handle the error
        }
        return conn;
    }

	public static ResultSet sqlQuery(String query, Connection conn) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
		    stmt = conn.createStatement();
		    rs = stmt.executeQuery(query);		    
		    
		    // Now do something with the ResultSet ....
		}
		catch (SQLException ex){
		    // handle any errors
		    
		    //System.out.println("SQLState: " + ex.getSQLState());
		    //System.out.println("VendorError: " + ex.getErrorCode());
		    System.out.println("SQL Exception: Please contact NAL IT to handle this error. It means that the database connection has not been established properly. Additional information is below.");
		      System.out.println("SQLException: " + ex.getMessage());
        }
		
		return rs;
		
	}
	
	public static ResultSet sqlQuery(String query, Connection conn, String host, String user) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
		    stmt = conn.createStatement();
		    rs = stmt.executeQuery(query);		    
		    
		    // Now do something with the ResultSet ....
		}
		catch (SQLException ex){
		    // handle any errors
		    
		    //System.out.println("SQLState: " + ex.getSQLState());
		    //System.out.println("VendorError: " + ex.getErrorCode());
		    System.out.println("SQL Exception: Please contact NAL IT to handle this error. It means that the database connection has not been established properly. Additional information is below.");
		      System.out.println("SQLException: " + ex.getMessage());
        }
		
		return rs;
		
	}
	
	
}
