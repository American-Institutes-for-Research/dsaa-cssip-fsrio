package com.jbt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

public class Defra {
	public static String main(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		Connection conn = MysqlConnect.connection(host,user,passwd);
		Defra.scrape(url,outfolder,conn,dbname);
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}		
		return "DEFRA";

	}

	public static void scrape(String url, String outfolder, Connection conn, String dbname) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"Defra_"+currentStamp+".csv"),'\t');
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"project__PROJECT_FUNDING",
				"institution_data__INSTITUTION_NAME","institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ADDRESS1","institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_ZIP",
				"institution_index__inst_id",
				"agency_index__aid","comment"};
		csvout.writeNext(header);
		
		WebClient webClient = new WebClient();
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		HtmlAnchor allResultsLink = startPage.getAnchorByText("View whole list");
		startPage = allResultsLink.click();
		
		Document doc = Jsoup.parse(startPage.asXml());
		Elements links = doc.select("a:containsOwn(Description)");
		for (Element link : links) {
			//Declare needed strings
			String project__PROJECT_NUMBER = "";
			String project__PROJECT_TITLE = "";
			String project__source_url = "";
			String project__PROJECT_START_DATE = "";
			String project__PROJECT_END_DATE = "";
			String project__PROJECT_MORE_INFO = "";
			String project__PROJECT_OBJECTIVE = "";
			String project__LAST_UPDATE = "";
			String project__DATE_ENTERED = "";
			String project__PROJECT_FUNDING = "";
			String agency_index__aid = "80";
			int institution_index__inst_id = -1;
			String comment = "";
			
			//Institution variables
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_COUNTRY = "184";
			String institution_data__INSTITUTION_ADDRESS1 = "";
			String institution_data__INSTITUTION_CITY = "";
			String institution_data__INSTITUTION_ZIP = "";
			
			//Processing variables
			String query = "";
			
			//Project source URL
			project__source_url = "http://randd.defra.gov.uk/"+link.attr("href");
			
			HtmlPage nextPage = webClient.getPage(project__source_url);
			Document finaldoc = Jsoup.parse(nextPage.asXml());
			
			finaldoc.select("br").remove();
			
			//Project number
			String titleNum = finaldoc.select("h3").last().text();
			project__PROJECT_NUMBER = titleNum.split(" - ")[titleNum.split(" - ").length-1];
			
			
			
			//Project title
			project__PROJECT_TITLE = titleNum.replace(" - "+project__PROJECT_NUMBER, "");
		
			//Project more info
			project__PROJECT_MORE_INFO = finaldoc.select("h5:containsOwn(Description)").first().parent().text().replace("Description ","");
			
			try {
				//Project objective
				project__PROJECT_OBJECTIVE = finaldoc.select("h5:containsOwn(Objective)").first().parent().text().replace("Objective ","");
			}
			catch (Exception e) {
				//No objective - perhaps just pass
			}
			// Start and end dates
			project__PROJECT_START_DATE = finaldoc.select("b:containsOwn(From:)").first().parent().text().replace("From: ","");
			project__PROJECT_END_DATE = finaldoc.select("b:containsOwn(To:)").first().parent().text().replace("To: ","");
			
			//Check if project exists in DB
			query = "SELECT PROJECT_NUMBER FROM "+dbname+".project where PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\""
					+ " and PROJECT_START_DATE = \""+project__PROJECT_START_DATE+"\" and PROJECT_END_DATE = \""+project__PROJECT_END_DATE+"\"";
			ResultSet result = MysqlConnect.sqlQuery(query,conn);
			try {
				result.next();
				String number = result.getString(1);
				continue;
			}
			catch (Exception ex) {;}

			
			//Date stamp
			project__LAST_UPDATE = dateFormat.format(current);
			DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
			project__DATE_ENTERED = dateFormatEnter.format(current);
			
			//Project funding
			project__PROJECT_FUNDING = finaldoc.select("b:containsOwn(Cost:)").first().parent().text().replace("Cost: ","").replace(",","").substring(1);
			
			//Institution name and URL
			Element instTab = finaldoc.select("h5:containsOwn(Contractor / Funded Organisations)").first().nextElementSibling();
			institution_data__INSTITUTION_NAME = instTab.text();
			
			//Check institution in MySQL DB
			query = "SELECT * from "+dbname+".institution_data where institution_name regexp \""+institution_data__INSTITUTION_NAME+"\"";
			result = MysqlConnect.sqlQuery(query,conn);
			try {
				result.next();
				institution_index__inst_id = result.getInt(1);
			}
			catch (Exception e) {
				Pattern patInst = Pattern.compile("\\((.*?)\\)");
				Matcher matchInst = patInst.matcher(institution_data__INSTITUTION_NAME);
				if (matchInst.find()) {
					//Check institution in MySQL DB (might be the one in parentheses)
					query = "SELECT * from "+dbname+".institution_data where institution_name regexp \""+matchInst.group(1)+"\"";
					result = MysqlConnect.sqlQuery(query,conn);
					try {
						result.next();
						institution_index__inst_id = result.getInt(1);
					}
					catch (Exception ee) {
						
						//Check institution in MySQL DB (might be the one in parentheses)
						try {
							query = "SELECT * from "+dbname+".institution_data where institution_name regexp \""+matchInst.group(1).split(" - ")[0]+"\"";
							result = MysqlConnect.sqlQuery(query,conn);
							result.next();
							institution_index__inst_id = result.getInt(1);
						}
						catch (Exception eee) {
							comment = "Please populate institution fields by exploring the institution named on the project.";
						}	
					}
				} else {
					//Check institution in MySQL DB (might be the one after dash)
					try {
						query = "SELECT * from "+dbname+".institution_data where institution_name regexp \""+institution_data__INSTITUTION_NAME.split(" - ")[1]+"\"";
						result = MysqlConnect.sqlQuery(query,conn);
						result.next();
						institution_index__inst_id = result.getInt(1);
					}
					catch (Exception ee) {
						comment = "Please populate institution fields by exploring the institution named on the project.";
					}
					
				}
			}

			
			//Write resultant values into CSV
			String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),project__source_url,
					project__PROJECT_START_DATE,project__PROJECT_END_DATE,
					project__PROJECT_MORE_INFO.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
					project__PROJECT_FUNDING,
					institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
					institution_data__INSTITUTION_COUNTRY.replaceAll("[\\n\\t\\r]"," "),
					institution_data__INSTITUTION_ADDRESS1,
					institution_data__INSTITUTION_CITY,
					institution_data__INSTITUTION_ZIP,
					String.valueOf(institution_index__inst_id),
					agency_index__aid,comment};
			
				csvout.writeNext(output);
		
		}
		
		csvout.close();
		webClient.close();
	
	}
	
}
