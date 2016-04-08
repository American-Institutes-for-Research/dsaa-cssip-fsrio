package com.jbt;

import net.sf.junidecode.Junidecode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

import java.net.URL;

/**
* This class scrapes six websites associated with the Agriculture and Horticulture Development Board.
* Returns void and writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
* 
* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the websites where web pages for individual projects can be found.
* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
* @see               Run
*/

public class AHDB {
	public static void ahdbMain(String outfolder, String[] links, String host, String user, String passwd, String dbname, String logfile) throws Exception {
		/**
		* The gargoylsoftware Web Client is very capricious and prints out every JavaScript error possible even when they are meaningless for the scraper.
		* We have to shut down the default logger to make our customized one provide more meaningful messages.
		*/
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		
		/**
		* Opening one connection per scraper, as instructed. 
		*/
		Connection conn = MysqlConnect.connection(host,user,passwd);
		
		/**
		* This scraper goes through six websites associated with the Agriculture and Horticulture Development Board.
		* All major weblinks are specified in the config file (typically, process.cfg) and can be retrieved/updated there.
		*/
		for (String link : links) {
			/**
			* The exception is thrown as a warning message for every method (website). 
			* It has been noticed that other system parameters, such as firewall settings, can be at fault.
			* It is recommended to re-run this data source at least one more to make sure that the error actually exists.
			*/
				
			if (link.replace("potatoes", "").length() < link.length()) {
				try {
					AHDB.potatoes(outfolder, link,conn, dbname);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB potatoes scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}
			}
			if (link.replace("horticulture", "").length() < link.length()) {
				try {
				AHDB.horticulture(outfolder, link,conn,dbname);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB horticulture scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}

			}
			if (link.replace("dairy", "").length() < link.length()) {
				try {
				AHDB.dairy(outfolder, link,conn,dbname);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB dairy scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}

			}
			if (link.replace("beefandlamb", "").length() < link.length()) {
				try {
				AHDB.meat(outfolder, link,conn,dbname,logfile);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB beef and lamb scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}

			}
			if (link.replace("cereals", "").length() < link.length()) {
				try {
				AHDB.cereals(outfolder, link,conn,dbname,logfile);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB cereals scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}

			}
			if (link.replace("pork", "").length() < link.length()) {
				try {
				AHDB.pork(outfolder,link,conn,dbname);
				} catch (Exception ex) {
					System.out.println("Warning: The AHDB pork scraper did not succeed. This error has not been seen before, i.e. not handled separately. Please share the following information with the IT support to troubleshoot:");
					ex.printStackTrace();
					System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
				}

			}
		}
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}		
	}

	public static void potatoes(String outfolder, String url,Connection conn, String dbname) throws Exception {
		/**
		* The date is needed in every subclass for logging and file naming purposes given that implement a customized logger for the most transparent and easiest error handling and troubleshooting.
		*/
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		/**
		* As seen, the naming convention for the output files is class, method, and current date. This is needed to impose version control and easier data organization for FSRIO staff.
		*/
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_potatoes_"+currentStamp+".csv"),'\t');
		
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__source_url", "project__PROJECT_START_DATE",
				"project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
				"agency_index__aid",
				"investigator_data__name", "institution_index__inst_id", "investigator_index__inv_id","institution_data__INSTITUTION_NAME" };
		csvout.writeNext(header);
		
		/**
		* The following code initiates the webClient and sets timeout at 50000 ms. 
		* Some websites, particularly Food Standards Agency are notorious for speed of response and require such high timeout value.
		*/
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		/**
		* Every web scraper consists of two parts:
		* 1) identify links to individual project pages
		* 2) scrape all necessary information from those individual project pages
		* <p>
		* The website structure is very different and it is impossible to template anything here from class extends and overrides.
		*/
		for (int i = 0; i < 4; i++ ){
			if(i != 0) {
				url = url + "&page=" + String.valueOf(i);
			}
			
			WebRequest webRequest = new WebRequest(new URL(url));
			webRequest.setCharset("utf-8");
			HtmlPage startPage = webClient.getPage(webRequest);
			Document doc = Jsoup.parse(startPage.asXml());
			
			/**
			* Here is where we finished Part 1: identifying links to individual project pages.
			*/
			Elements links = doc.select("li[class=listing-publication").select("div[class=pub-content]").select("a");
			
			for (Element link: links) {
				/**
				* In this website, the list of project links can also have some PDF files that are reports. We have to pass them here.
				*/
				if (link.attr("href").contains(".pdf")) continue;
				
				/**
				* Every web scraper declares a list of variables that are present in project web pages. 
				* It is important that different websites can have different lists of data fields. That explains why we do not template, extend and override.
				*/
				String project__PROJECT_NUMBER = "";
				String project__PROJECT_TITLE = "";
				String project__source_url = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project__PROJECT_OBJECTIVE = "";
				String project__LAST_UPDATE = "";
				String project__DATE_ENTERED = "";
				String project__PROJECT_FUNDING = "";
				String agency_index__aid = "146";
				String investigator_data__name = "";
				String institution_data__INSTITUTION_NAME = "";
				int institution_index__inst_id = -1;
				int investigator_index__inv_id = -1;
				String comment = "";
				
				/**
				*Processing variables
				*/
				String piName = "";
				String piLastName = "";
				String piFirstName = "";
				
				/**
				* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
				*/
				project__source_url = "http://potatoes.ahdb.org.uk/" + link.attr("href");
				
				Document finaldoc = Jsoup.connect(project__source_url).timeout(50000).get();
				project__PROJECT_TITLE = finaldoc.select(":containsOwn(Full Research Project Title)").text().replace("Full Research Project Title:", "").trim();
				project__PROJECT_NUMBER = finaldoc.select("h1[id=page-title]").text().split(" ")[0];
				if (project__PROJECT_TITLE.equals("")) {
					project__PROJECT_TITLE = finaldoc.select("h1[id=page-title]").text().replace(project__PROJECT_NUMBER+" ", "");
				}
				
				/**
				* Sometimes there's no project number and therefore the link need to be passed
				*/
				Pattern checkProjNum = Pattern.compile("\\d+");
				Matcher matchProjNum = checkProjNum.matcher(project__PROJECT_NUMBER);
				if (!matchProjNum.find()) {
					continue;
				}
				
				/**
				* Parse investigator name in correct format: [Last Name], [First Name]
				*/
				piName = finaldoc.select("div[class=field field-name-field-author field-type-text field-label-inline clearfix]").select("div[class=field-item even]").text();
				piLastName = piName.split(" ")[piName.split(" ").length-1];
				piFirstName = piName.replace(" "+piLastName,"");
				investigator_data__name = piLastName+", "+piFirstName;
				
				
				project__PROJECT_OBJECTIVE = finaldoc.select("div[class=field field-name-body field-type-text-with-summary field-label-hidden]").select("div[class=field-item even]").text();
				String duration = finaldoc.select("div[class=field field-name-field-headline field-type-text field-label-hidden]").select("div[class=field-item odd]").text();
				Pattern p = Pattern.compile("\\d+");
				Matcher matcher = p.matcher(duration);
				if (matcher.find()) {
					project__PROJECT_START_DATE = matcher.group();
				}
				if (matcher.find()) {
					project__PROJECT_END_DATE = matcher.group();
				}
				
				
				/**
				* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: project number, project start date, project end date, institution names and/or PI name (if applicable).
				* This is exactly what the following MySQL queries are doing.
				*/
				
				String GetInvestigatorSQL = "SELECT ID FROM  "+dbname+"investigator_data WHERE NAME LIKE ?;";
				/**
				* Check if investigator exists in the current FSRIO Research Projects Database.
				* Exception is implemented to try different variants because of spelling mistakes possible in PI names.
				* The last Exception except here needs to be ignored because by default we assume that the PI does not exist in the database and assign -1 index.
				*/
				
				ResultSet rs6 = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
					preparedStmt.setString(1, investigator_data__name);
					rs6 = preparedStmt.executeQuery();
					rs6.next();
					investigator_index__inv_id = rs6.getInt(1);
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
					catch (Exception except) {
						
					}
				}

				
				if (investigator_index__inv_id == -1) {

				} else {

					/**
					* Check if project exists in DB
					*/
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
				project__LAST_UPDATE = dateFormat.format(current);
				DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
				project__DATE_ENTERED = dateFormatEnter.format(current);
				
				/**
				* Parse institution name - might be several
				*/
				String[] institutions = finaldoc.select("div[class=field field-name-field-contractor field-type-text field-label-inline clearfix]").select("div[class=field-item even]").text().split(", | & ");
				for (String inst : institutions ) {
					institution_data__INSTITUTION_NAME = inst;
					
					/** 
					* Check if institution exists in DB
					* By default we assume that it does not exist in the DB. 
					* (Exception e) adds a comment to output spreadsheet because some institution fields are not present at the web page but FSRIO might want to retrieve additional information from elsewhere on the Internet.
					*/
					String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
					ResultSet rs = null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
						preparedStmt.setString(1, institution_data__INSTITUTION_NAME); 
						rs = preparedStmt.executeQuery();

						rs.next();
						institution_index__inst_id = rs.getInt(1);
					}
					catch (Exception e) {
						comment = "Please populate institution fields by exploring the institution named on the project.";
					}

					
					
					/**
					* Outputting all data into tab-separated file. 
					* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
					*/
					String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "), 
							project__source_url, project__PROJECT_START_DATE,
							project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
							agency_index__aid,
							investigator_data__name.replaceAll("[\\n\\t\\r]"," "), String.valueOf(institution_index__inst_id), String.valueOf(investigator_index__inv_id),institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," ") };
					csvout.writeNext(output);
				}
		}
		}
		webClient.close();
		csvout.close();
	}
	
	public static void horticulture(String outfolder, String url, Connection conn, String dbname) throws Exception {
		/**
		* The date is needed in every subclass for logging and file naming purposes given that implement a customized logger for the most transparent and easiest error handling and troubleshooting.
		*/
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		/**
		* As seen, the naming convention for the output files is class, method, and current date. This is needed to impose version control and easier data organization for FSRIO staff.
		*/
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_horticulture_"+currentStamp+".csv"),'\t');
		
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__source_url", "project__PROJECT_START_DATE",
				"project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
			    "agency_index__aid",
				"investigator_data__name", "institution_index__inst_id", "investigator_index__inv_id", 
				"institution_data__INSTITUTION_NAME"};
		csvout.writeNext(header);
		
		/**
		* The following code initiates the webClient and sets timeout at 50000 ms. 
		* Some websites, particularly Food Standards Agency are notorious for speed of response and require such high timeout value.
		*/
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		/**
		* Every web scraper consists of two parts:
		* 1) identify links to individual project pages
		* 2) scrape all necessary information from those individual project pages
		* <p>
		* The website structure is very different and it is impossible to template anything here from class extends and overrides.
		*/
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		
		
		int nPages = doc.select("ul[class=pager]").select("li[class=pager-item").size();
		String url2 = "";
		for (int i = 0; i <= nPages; ++i) {
			if(i != 0) {
			    url2 = url + "?page=" + String.valueOf(i);
			   }
			   else {
			    url2 = url;
			   }
			startPage = webClient.getPage(url2);

			doc = Jsoup.parse(startPage.asXml());
			
			/**
			* Here is where we finished Part 1: identifying links to individual project pages.
			*/
			Elements links = doc.select("article").select("li[class=node-readmore first last]").select("a");

			for(Element link: links) {
				/**
				* Every web scraper declares a list of variables that are present in project web pages. 
				* It is important that different websites can have different lists of data fields. That explains why we do not template, extend and override.
				*/
				String project__PROJECT_NUMBER = "";
				String project__PROJECT_TITLE = "";
				String project__source_url = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project__PROJECT_OBJECTIVE = "";
				String project__PROJECT_MORE_INFO = "";
				String project__LAST_UPDATE = "";
				String project__DATE_ENTERED = "";
				String project__PROJECT_FUNDING = "";
				String agency_index__aid = "146";
				String investigator_data__name = "";
				String institution_data__INSTITUTION_NAME = "";
				int institution_index__inst_id = -1;
				int investigator_index__inv_id = -1;
				String comment = "";
				
				/**
				*Processing variables
				*/
				String piName = null;
				String piLastName = null;
				String piFirstName = null;
				
				/**
				* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
				*/
				project__source_url = "http://horticulture.ahdb.org.uk/" + link.attr("href");
				
				Document finaldoc = Jsoup.connect(project__source_url).timeout(50000).get();
				project__PROJECT_TITLE = finaldoc.select("span[property=dc:title]").attr("content");
				project__PROJECT_NUMBER = finaldoc.select("header").select("h2").text().split("-")[0].replace("HDC user info and login Search form ", "");
				project__PROJECT_START_DATE = finaldoc.select("div[class=content]").select("div[class=field field-name-field-start-date field-type-datetime field-label-inline clearfix]").select("span").attr("content").substring(0, 4);
				project__PROJECT_END_DATE = finaldoc.select("div[class=content]").select("div[class=field field-name-field-release-date field-type-datetime field-label-inline clearfix]").select("span").attr("content").substring(0, 4);
				String temp = finaldoc.select("div[class=content]").select("div[class=field field-name-field-author field-type-text field-label-inline clearfix]").select("div[class=field-item even]").text();
				
				/**
				* Parse investigator name in correct format
				*/
				piName = temp.split(",")[0].replace(" Warwick Crop Centre","");
				Pattern patName = Pattern.compile("^Prof |^Professor |^Dr. |^Doctor |^Dr |^Ms |^Mrs |^Mr ");
				Matcher matchName = patName.matcher(piName);
				piName = matchName.replaceAll("");
				piLastName = piName.split(" ")[piName.split(" ").length-1];
				piFirstName = piName.replace(" "+piLastName,"");
				investigator_data__name = piLastName+", "+piFirstName;
				
				/** 
				* It is normal at this website not to have institution information and it is not critical. So, no need to distract the flow by meaningful exception handler.
				*/
				try {
					institution_data__INSTITUTION_NAME = temp.split(", ")[1];	
				}
				catch (Exception e) {;}
				
				if (temp.split(",")[0].contains("Warwick Crop Centre")) {
					institution_data__INSTITUTION_NAME = "Warwick Crop Centre";
				}
				
				/**
				* Parsing project objective - hard to deal with bad structure 
				* including industry rep and cost fields that need to be removed
				*/
				Element projDoc = 
						finaldoc.select("div[class=content]").select("div[class=field field-name-body field-type-text-with-summary field-label-hidden]").first().children().first().children().first();
				Pattern badObj = Pattern.compile("cost:|cost\\):|industry rep",Pattern.CASE_INSENSITIVE);
				Matcher matchBad = badObj.matcher(projDoc.text());
				if (matchBad.find()) {
					Elements projP = projDoc.children().select("*");
					Pattern patObj = Pattern.compile("summary|problem|objective|aphids",Pattern.CASE_INSENSITIVE);
					for (Element p : projP) {
						Matcher matchObj = patObj.matcher(p.text());
						if (!matchObj.find()) {
							p.remove();
						} else {
							break;
						}
					}
				}
				project__PROJECT_OBJECTIVE = projDoc.text();
				
				project__PROJECT_NUMBER = finaldoc.select("header").select("h2").text().replaceAll("HDC user info and login Search form", "").split("-")[0].trim();
				project__LAST_UPDATE = dateFormat.format(current);
				DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
				project__DATE_ENTERED = dateFormatEnter.format(current);
				
				/**
				* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: project number, project start date, project end date, institution names and/or PI name (if applicable).
				* This is exactly what the following MySQL queries are doing.
				*/
				
				String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
				/** 
				* Check if institution exists in DB
				* By default we assume that it does not exist in the DB. 
				* (Exception e) adds a comment to output spreadsheet because some institution fields are not present at the web page but FSRIO might want to retrieve additional information from elsewhere on the Internet.
				*/
				ResultSet rs = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
					preparedStmt.setString(1, investigator_data__name);
					rs = preparedStmt.executeQuery();
					rs.next();
					institution_index__inst_id = rs.getInt(1);
				}
				catch (Exception e) {
					comment = "Please populate institution fields by exploring the institution named on the project.";
				}
				
				/**
				* Check if investigator exists in the current FSRIO Research Projects Database.
				* Exception is implemented to try different variants because of spelling mistakes possible in PI names.
				* The last Exception except here needs to be ignored because by default we assume that the PI does not exist in the database and assign -1 index.
				*/
				String GetInvestigatorSQL = "SELECT ID FROM  "+dbname+"investigator_data WHERE NAME LIKE ?;";
				ResultSet rs6 = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
					preparedStmt.setString(1, investigator_data__name);
					rs6 = preparedStmt.executeQuery();
					rs6.next();
					investigator_index__inv_id = rs6.getInt(1);
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
					catch (Exception except) {
						
					}	
				}

				
				if (investigator_index__inv_id == -1) {
				} else {

					/**
					* Check if project exists in DB. Again, exception needs to be ignored.
					*/
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

				/**
				* Outputting all data into tab-separated file. 
				* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
				*/
				String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "), 
						project__source_url, project__PROJECT_START_DATE,
						project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
						agency_index__aid,
						investigator_data__name, String.valueOf(institution_index__inst_id), String.valueOf(investigator_index__inv_id), 
						institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," ")};
				csvout.writeNext(output);
				}
			
		}
		csvout.close();

	}

	public static void dairy(String outfolder, String url, Connection conn, String dbname) throws Exception {
		
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_dairy_"+currentStamp+".csv"),'\t');
		
		String[] header = {"project__PROJECT_TITLE", 
				"project__source_url", "project__PROJECT_START_DATE",
				"project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
			    "agency_index__aid",
				"institution_index__inst_id", "institution_data__INSTITUTION_NAME" };
		csvout.writeNext(header);

		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		Elements links = doc.select("a");
		int i = 0;
		for (Element link: links) {
			if (!(link.attr("href").contains("current-projects"))) {
				continue;
			}
			if (i <2 ) {i+=1; continue;}
			String project__PROJECT_NUMBER = "";
			String project__PROJECT_TITLE = "";
			String project__source_url = "";
			String project__PROJECT_START_DATE = "";
			String project__PROJECT_END_DATE = "";
			String project__PROJECT_OBJECTIVE = "";
			String project__LAST_UPDATE = "";
			String project__DATE_ENTERED = "";
			String project__PROJECT_FUNDING = "";
			String agency_index__aid = "146";
			String investigator_data__name = "";
			int institution_index__inst_id = -1;
			int investigator_index__inv_id = -1;
			String comment = "";
			
			
			//Institution variables
			String institution_data__INSTITUTION_NAME = "";
			Document finaldoc = null;
			project__source_url = "http://dairy.ahdb.org.uk/" + link.attr("href");
			try {
				finaldoc = Jsoup.connect(project__source_url).timeout(50000).get();
			}
			catch (Exception eee) {
				HtmlPage startPageProj = webClient.getPage(project__source_url);
				finaldoc = Jsoup.parse(startPageProj.asXml());
			}
			project__PROJECT_TITLE = finaldoc.select("div[class=column2]").select("h3").text();
			
			//Project dates
			String temp = StringEscapeUtils.unescapeHtml4(finaldoc.select("p,ul").text());
			try {
				project__PROJECT_START_DATE = temp.substring(temp.toLowerCase().indexOf("start date"), temp.toLowerCase().indexOf("completion date")).replaceAll("\\D+", "");	
			}
			catch(Exception e) {
				try {
					project__PROJECT_START_DATE = temp.substring(temp.toLowerCase().indexOf("start"), temp.toLowerCase().indexOf("completion")).replaceAll("\\D+", "");
				} catch (Exception ee) {
					;
				}
			}
			try {
				project__PROJECT_END_DATE = temp.substring(temp.toLowerCase().indexOf("completion date"), temp.toLowerCase().indexOf("lead contractor")).replaceAll("\\D+", "");;
			}
			catch(Exception e) {
				try {
					project__PROJECT_END_DATE = temp.substring(temp.toLowerCase().indexOf("completion"), temp.toLowerCase().indexOf("lead")).replaceAll("\\D+", "");
				} catch (Exception ee) {
					;
				}
			}
			
			//Institution info
			try {
				institution_data__INSTITUTION_NAME = temp.substring(temp.indexOf("Lead Contractor"), temp.toLowerCase().indexOf("other delivery")).replace("Lead Contractor", "").replace("- Research Partnership study", "").trim();
			}
			catch(Exception e) {;}
			if (institution_data__INSTITUTION_NAME == "") {
				try {
					
				institution_data__INSTITUTION_NAME = temp.substring(temp.indexOf("Lead Contractor"), temp.indexOf("Funder")).replace("Lead Contractor", "").replace("- Research Partnership study", "").trim();
			}
			catch(Exception e) {;}
			}
			temp = finaldoc.text();
			try {
				project__PROJECT_OBJECTIVE = temp.substring(temp.toLowerCase().indexOf("aims"), temp.toLowerCase().indexOf("start")).replaceAll("Aims & Objectives", "");	
			}
			catch (Exception e) {e.printStackTrace();
			System.out.println(project__source_url);
			System.out.println(temp.toLowerCase());}
			project__LAST_UPDATE = dateFormat.format(current);
			DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
			project__DATE_ENTERED = dateFormatEnter.format(current);
			
			// Find institution
			String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
			ResultSet rs = null;
			try {
				PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
				preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
				rs = preparedStmt.executeQuery();
				rs.next();
				institution_index__inst_id = rs.getInt(1);
			}
			catch (Exception e) {
				comment = "Please populate institution fields by exploring the institution named on the project.";
			}

			//Check if project exists in DB
			String query = "SELECT PROJECT_TITLE FROM  "+dbname+"project where PROJECT_TITLE = ?"
					+ " and PROJECT_START_DATE = ? and PROJECT_END_DATE = ?";
			ResultSet result = null;
			try {
				PreparedStatement preparedStmt = conn.prepareStatement(query);
				preparedStmt.setString(1, project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "));
				preparedStmt.setString(2, project__PROJECT_START_DATE);
				preparedStmt.setString(3, project__PROJECT_END_DATE);

				result = preparedStmt.executeQuery();
				result.next();
				String number = result.getString(1);
				continue;
			}
			catch (Exception ex) {;}

			
			String[] output = {project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "), 
					project__source_url, project__PROJECT_START_DATE,
					project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
					agency_index__aid,
					String.valueOf(institution_index__inst_id), institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," ") };
			csvout.writeNext(output);
			
		}
		csvout.close();
	}
	
	public static void meat(String outfolder, String url, Connection conn, String dbname, String logfile) throws Exception {
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_meat_"+currentStamp+".csv"),'\t');
		
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__source_url", "project__PROJECT_START_DATE",
				"project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE","project__PROJECT_MORE_INFO",
				"agency_index__aid",
				 "institution_index__inst_id", "institution_data__INSTITUTION_NAME" };
		csvout.writeNext(header);
		
		String beef = url + "/meat-eating-quality-and-safety-beef/";
		String sheep = url + "/meat-eating-quality-and-safety-sheep/";
		String generic = url + "/meat-eating-quality-and-safety-generic/";
		String []  urls = {beef, sheep, generic};
		
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		for (String url1: urls) {
			try {
				HtmlPage startPage = null;
				Document finaldoc = null;
				startPage = webClient.getPage(url1);
				finaldoc = Jsoup.parse(startPage.asXml());
		
				Elements links = finaldoc.select("li").select("a[title!=\"\"]");
				for (Element link : links) {
					String project__PROJECT_NUMBER = "";
					String project__PROJECT_TITLE = "";
					String project__source_url = "";
					String project__PROJECT_START_DATE = "";
					String project__PROJECT_END_DATE = "";
					String project__PROJECT_OBJECTIVE = "";
					String project__PROJECT_MORE_INFO = "";
					String project__LAST_UPDATE = "";
					String project__DATE_ENTERED = "";
					String project__PROJECT_FUNDING = "";
					String agency_index__aid = "146";
					String investigator_data__name = "";
					int institution_index__inst_id = -1;
					int investigator_index__inv_id = -1;
					String comment = "";
					
					
					//Institution variables
					String institution_data__INSTITUTION_NAME = "";
					
					//Project URL
					project__source_url = link.attr("href");
					
					startPage = webClient.getPage(project__source_url);
					finaldoc = Jsoup.parse(startPage.asXml());
					
					project__PROJECT_NUMBER = finaldoc.select(":containsOwn(project number)").text();
					project__PROJECT_NUMBER = project__PROJECT_NUMBER.toLowerCase().replaceAll("project number", "").replace(":", "").trim();
					project__PROJECT_NUMBER = project__PROJECT_NUMBER.replaceAll(String.valueOf((char) 160), "");
					
					
					String temp = finaldoc.select("div[class=entry-content]").text();
					try {
						project__PROJECT_TITLE = temp.substring(0, temp.toLowerCase().indexOf("project number"));	
					}
					catch(Exception e) {
						project__PROJECT_TITLE = finaldoc.select("article").select("h1[class=entry-title]").text();
					}
					if (project__PROJECT_TITLE.equals("")) 
						project__PROJECT_TITLE = finaldoc.select("title").text();
					institution_data__INSTITUTION_NAME = finaldoc.select(":containsOwn(lead contractor)").text();
					institution_data__INSTITUTION_NAME = institution_data__INSTITUTION_NAME.toLowerCase().replaceAll("lead contractor", "").replace(":", "").trim();
					institution_data__INSTITUTION_NAME = institution_data__INSTITUTION_NAME.replaceAll(String.valueOf((char) 160), "");
					institution_data__INSTITUTION_NAME = WordUtils.capitalize(institution_data__INSTITUTION_NAME, '-',' ','(').replace("Of","of").replace("And", "and").trim();
					String duration = finaldoc.select(":containsOwn(start & end date)").text();
					Pattern p = Pattern.compile("\\d{4}");
					Pattern p2 = Pattern.compile("/(\\d{2})\\s.*?/(\\d{2})$");
					Matcher matcher = p.matcher(duration);
					Matcher matcher2 = p2.matcher(duration);
					if (matcher.find()) {
						project__PROJECT_START_DATE = matcher.group();
					} 
					if (matcher.find()) {
						project__PROJECT_END_DATE = matcher.group();
					}
					
					if (project__PROJECT_START_DATE.equals("")) {
						if (matcher2.find()) {
							project__PROJECT_START_DATE = "20"+matcher2.group(1);
							project__PROJECT_END_DATE = "20"+matcher2.group(2);
						}
					}
					
					//Project objective
					Elements projInfo = finaldoc.select("div.entry-content").first().select("p");
					int i = 0;
					int Obj = 0;
					int MoreInfo = 0;
					for (i=0;i<projInfo.size();i++) {
						Element nextSib = projInfo.get(i);
						if (nextSib.text().contains("The Problem") || nextSib.text().contains("T he Problem")) {
							Obj = i+1;
						} 
						if (nextSib.text().contains("Approach")) {
							MoreInfo = i+1;
						}  
					}
					
					for (i=Obj;i<MoreInfo-1;i++) {
						Element nextSib = projInfo.get(i);
						project__PROJECT_OBJECTIVE+= nextSib.text()+" ";
					}
					
					for (i=MoreInfo;i<projInfo.size();i++) {
						Element nextSib = projInfo.get(i);
						project__PROJECT_MORE_INFO+= nextSib.text()+" ";
					}
					
					// Find institution
					String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
					ResultSet rs = null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
						preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
						rs = preparedStmt.executeQuery();
						rs.next();
						institution_index__inst_id = rs.getInt(1);
					}
					catch (Exception e) {
						comment = "Please populate institution fields by exploring the institution named on the project.";
					}

					//Check if project exists in DB
					String query = "SELECT PROJECT_NUMBER FROM  "+dbname+"project where PROJECT_NUMBER = ?"
							+ " and PROJECT_START_DATE = ? and PROJECT_END_DATE = ?;";
					ResultSet result =null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(query);
						preparedStmt.setString(1, project__PROJECT_NUMBER);
						preparedStmt.setString(2, project__PROJECT_START_DATE);
						preparedStmt.setString(3, project__PROJECT_END_DATE);
						result = preparedStmt.executeQuery();
						result.next();
						String number = result.getString(1);
						continue;
					}
					catch (Exception ex) {;}

					
					project__LAST_UPDATE = dateFormat.format(current);
					DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
					project__DATE_ENTERED = dateFormatEnter.format(current);
					String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "), 
							project__source_url, project__PROJECT_START_DATE,
							project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
							project__PROJECT_MORE_INFO.replaceAll("[\\n\\t\\r]"," "),
							agency_index__aid,
							String.valueOf(institution_index__inst_id), institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," ")};
					csvout.writeNext(output);
					
				}
			}
			catch (Exception eee) {
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
					StringWriter errors = new StringWriter();
					eee.printStackTrace(new PrintWriter(errors));
					out.println(currentDateLog
				    			+"   "
				    			+"Perhaps the link is broken or does not exist - "+url1+" ."
				    			+" Here is some help with traceback:"
				    			+errors.toString());
				}catch (IOException e) {

				}
			}
			
		}
		csvout.close();
		webClient.close();
		
		
	}
	
	public static void cereals(String outfolder, String url, Connection conn, String dbname, String logfile) throws Exception {
		String weed = url.replace("disease", "weed");
		String pest = url.replace("disease", "pest");
		String nutrient = url.replace("disease", "nutrient");
		String soil = url.replace("disease", "soil");
		String environment = url.replace("disease", "environment");
		String grain = url.replace("disease", "grain-quality");

		String urls [] = {url, weed, pest, nutrient, soil, environment, grain};
		
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_cereals_"+currentStamp+".csv"),'\t');
		
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", 
				"project__source_url", "project__PROJECT_START_DATE",
				"project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
				"agency_index__aid",
				"investigator_data__name", "investigator_index__inv_id", "institution_index__inst_id" , 
				"institution_data__INSTITUTION_NAME"};
		csvout.writeNext(header);
		
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		for (String url1: urls) {
			HtmlPage startPage = webClient.getPage(url1);
			Document doc = Jsoup.parse(startPage.asXml());
			Set<String> links = new HashSet<String>();
			//links.addAll(doc.select("section[class=block]").select("a"));
			Elements linkWeb = doc.select("section[class=block]").select("a");
			for (Element link : linkWeb) {
				links.add(link.attr("href"));
			}
			for (String link : links) {
				String project__PROJECT_NUMBER = "";
				String project__PROJECT_TITLE = "";
				String project__source_url = "";
				String project__PROJECT_START_DATE = "";
				String project__PROJECT_END_DATE = "";
				String project__PROJECT_OBJECTIVE = "";
				String project__LAST_UPDATE = "";
				String project__DATE_ENTERED = "";
				String project__PROJECT_FUNDING = "";
				String agency_index__aid = "146";
				String investigator_data__name = "";
				int institution_index__inst_id = -1;
				int investigator_index__inv_id = -1;
				String comment = "";
				String lastName = "";
				String firstName = "";
				
				//Institution variables
				String institution_data__INSTITUTION_NAME = "";
				project__source_url = "http://cereals.ahdb.org.uk" + link;

				HtmlPage t = webClient.getPage(project__source_url);
				Document finaldoc = Jsoup.parse(t.asXml());
				try {
					
					project__PROJECT_NUMBER = finaldoc.select("strong:containsOwn(Project number)").first().parents().first().nextElementSibling().text();
					if (project__PROJECT_NUMBER.equals("The challenge"))
						throw new IOException();
					project__PROJECT_TITLE = finaldoc.select("article").select("header").select("h2").first().text();

					//Project start and end dates
					project__PROJECT_START_DATE = finaldoc.select("strong:containsOwn(Start)").first().parents().first().nextElementSibling().text();
					project__PROJECT_START_DATE = project__PROJECT_START_DATE.substring(project__PROJECT_START_DATE.length()-4);
					project__PROJECT_END_DATE = finaldoc.select("strong:containsOwn(End)").first().parents().first().nextElementSibling().text();
					project__PROJECT_END_DATE = project__PROJECT_END_DATE.substring(project__PROJECT_END_DATE.length()-4);
					
					//Inst and PI info
					String instInfo = null;
					try {
						instInfo = finaldoc.select("strong:containsOwn(Lead)").first().parents().first().nextElementSibling().text().split(";")[0];
					} 
					catch (Exception e) {
						try {
							instInfo = finaldoc.select("strong:containsOwn(PhD)").first().parents().first().nextElementSibling().text().split(";")[0];
						} catch (Exception ee) {
							try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
								StringWriter errors = new StringWriter();
								e.printStackTrace(new PrintWriter(errors));
								out.println(currentDateLog
							    			+"   "
							    			+"Institution and PI info do not scrape correctly here; need to check "
							    			+ "at "+project__source_url+" ."
							    			+" Here is some help with traceback:"
							    			+errors.toString());
								continue;
							}catch (IOException eee) {

							}
						}
					}
					
					investigator_data__name = instInfo.split(",")[0];
					if (instInfo.split(",").length > 1) {
						institution_data__INSTITUTION_NAME = instInfo.split(",")[1].trim();
					} else {
						if (instInfo.split(" \\(").length > 1) {
							investigator_data__name = instInfo.split(" \\(")[0];
							institution_data__INSTITUTION_NAME = instInfo.split(" \\(")[1].replace(")", "");
						} else {
							if (investigator_data__name.contains("ADAS")) {
								institution_data__INSTITUTION_NAME = "ADAS";
								investigator_data__name = investigator_data__name.replace("(ADAS)","").replace("ADAS","");
							} else {
								institution_data__INSTITUTION_NAME = investigator_data__name;
								investigator_data__name = "";
							}
						}
					}
					
					if (investigator_data__name.contains("Ltd")) {
						investigator_data__name = "";
					}
					
					if (!investigator_data__name.equals("")) {
						investigator_data__name = investigator_data__name.replaceAll("Dr |Prof |Mr |Ms |Miss |Mrs ", "");
						lastName = investigator_data__name.split(" ")[investigator_data__name.split(" ").length-1];
						firstName = investigator_data__name.replace(" "+lastName, "");
						investigator_data__name = lastName+", "+firstName;
					}
				}
				
				catch(Exception e) {
					try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
						StringWriter errors = new StringWriter();
						e.printStackTrace(new PrintWriter(errors));
						out.println(currentDateLog
					    			+"   "
					    			+"Something went terribly wrong with scraping here "
					    			+ "or maybe just a generic link to Related publications "
					    			+ "that's not needed here - check at "+project__source_url+" ."
					    			+" Here is some help with traceback:"
					    			+errors.toString());
						continue;
					}catch (IOException ee) {

					}
				}
				
				if (project__PROJECT_END_DATE.contains("/")) {
					project__PROJECT_END_DATE =  project__PROJECT_END_DATE.substring(project__PROJECT_END_DATE.length()-2);
					project__PROJECT_END_DATE = "20" + project__PROJECT_END_DATE;
				}
				if (project__PROJECT_START_DATE.contains("/")) {
					project__PROJECT_START_DATE =  project__PROJECT_START_DATE.substring(project__PROJECT_START_DATE.length()-2);
					project__PROJECT_START_DATE = "20" + project__PROJECT_START_DATE;
				}
				
				
				// Find institution in DB
				String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
				ResultSet rs = null;
				try {
					PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
					preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
					rs = preparedStmt.executeQuery();
					rs.next();
					institution_index__inst_id = rs.getInt(1);
				}
				catch (Exception e) {
					comment = "Please populate institution fields by exploring the institution named on the project.";
				}

				if (!investigator_data__name.equals("")) {
					//Find investigator
					String GetInvestigatorSQL = "SELECT ID FROM  "+dbname+"investigator_data WHERE NAME LIKE ?;";
					ResultSet rs6 = null;
					try {
						PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
						preparedStmt.setString(1, investigator_data__name);
						rs6 = preparedStmt.executeQuery();
						rs6.next();
						investigator_index__inv_id = rs6.getInt(1);
					}
					catch (Exception e) {
						String query = "SELECT * FROM  "+dbname+"investigator_data where name regexp ^?;";
						ResultSet result = null;
						try {
							PreparedStatement preparedStmt = conn.prepareStatement(query);
							preparedStmt.setString(1, lastName+", "+firstName.substring(0,1));
							result = preparedStmt.executeQuery();
							result.next();
							investigator_index__inv_id = result.getInt(1);
						}
						catch (Exception except) {
							if (!comment.equals("")) {
								comment = "Please populate institution and PI fields by exploring at the "+project__source_url;
							} else {
								comment = "Please populate PI information by exploring at the "+project__source_url;
							}
						}	
						
					}

				}
				
				if (investigator_index__inv_id == -1) {
				} else {

					//Check if project exists in DB
					String query = "SELECT p.PROJECT_NUMBER FROM  "+dbname+"project p left outer join  "+dbname+"investigator_index ii on ii.pid = p.id where p.PROJECT_NUMBER = ?"
							+ " and p.PROJECT_START_DATE = ? and p.PROJECT_END_DATE =  ? and ii.inv_id = ?;";
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
				project__LAST_UPDATE = dateFormat.format(current);
				DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
				project__DATE_ENTERED = dateFormatEnter.format(current);
				project__PROJECT_OBJECTIVE =finaldoc.select("p").text().trim(); 
				String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "), project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "), 
						project__source_url, project__PROJECT_START_DATE,
						project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
						agency_index__aid,
						investigator_data__name, String.valueOf(investigator_index__inv_id), String.valueOf(institution_index__inst_id), 
						institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," ")};
				csvout.writeNext(output);
				
			
			}
		}
		csvout.close();
	}
	
	public static void pork(String outfolder, String url, Connection conn, String dbname) throws Exception {
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"AHDB_pork_"+currentStamp+".csv"),'\t');
		
		// There is no project number.
		String[] header = {"project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_index__inst_id",
				"agency_index__aid"};
		csvout.writeNext(header);
		
		WebClient webClient = new WebClient();
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		Elements links = doc.select("div[id=innerNav]").select("li[class=current Page]").select("a");
		int i = 0;
		for (Element link: links) {
			String project__PROJECT_NUMBER = "";
			String project__PROJECT_TITLE = "";
			String project__source_url = "";
			String project__PROJECT_START_DATE = "";
			String project__PROJECT_END_DATE = "";
			String project__PROJECT_OBJECTIVE = "";
			String project__LAST_UPDATE = "";
			String project__DATE_ENTERED = "";
			String project__PROJECT_FUNDING = "";
			String agency_index__aid = "147";
			String investigator_data__name = "";
			int institution_index__inst_id = -1;
			String institution_data__INSTITUTION_NAME = "";
			int investigator_index__inv_id = -1;
			String comment = "";

			//Project source URL
			if (i == 0) {
				i++;
				continue;
			}
			project__source_url = "http://pork.ahdb.org.uk/"+link.attr("href");
			Document finaldoc = Jsoup.connect(project__source_url).timeout(50000).get();
			project__PROJECT_TITLE = finaldoc.select("span[class=link active]").text();
			project__PROJECT_OBJECTIVE = finaldoc.select("article").text();
			
			//Get 
			Elements t = finaldoc.select("article").select("p, ul, li");
			String duration = t.select(":containsOwn(Duration)").text().replace("Duration:", "").trim();
			duration = Junidecode.unidecode(duration);
			try {
				project__PROJECT_START_DATE = duration.split("-")[0].trim();
				project__PROJECT_END_DATE = duration.split("-")[1].trim();
			}
			catch(Exception e) {;}
			investigator_data__name = t.select(":containsOwn(AHDB Pork-funded studentship)").text();
			investigator_data__name = investigator_data__name.substring(investigator_data__name.indexOf("(")+1,investigator_data__name.indexOf(")"));
			try {
				institution_data__INSTITUTION_NAME = t.select(":containsOwn(Research partner)").text().split(":")[1].trim();
			}
			catch(Exception e) {;}
			String text = t.text().toLowerCase();
			try {
				project__PROJECT_OBJECTIVE = t.text().substring(text.toLowerCase().indexOf("aims and obj"), t.text().indexOf("Findings")-1);
				project__PROJECT_OBJECTIVE = project__PROJECT_OBJECTIVE.replace("Aims and objectives", "");
			}
			catch(Exception e) {;}
			
			// Get institution Id, if exists
			String GetInstIDsql = "SELECT ID FROM  "+dbname+"institution_data WHERE INSTITUTION_NAME LIKE ?;";
			ResultSet rs = null;
			try {
				PreparedStatement preparedStmt = conn.prepareStatement(GetInstIDsql);
				preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
				rs = preparedStmt.executeQuery();
				rs.next();
				institution_index__inst_id = rs.getInt(1);
			}
			catch (Exception e) {
				comment = "Please populate institution fields by exploring the institution named on the project.";
			}

			// See if PI exists.
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
				
			}

			//Check if project exists in DB
			String query = "SELECT PROJECT_NUMBER FROM  "+dbname+"project where PROJECT_NUMBER = ?"
					+ " and PROJECT_START_DATE =  ? and PROJECT_END_DATE = /;";
			ResultSet result = null;
			try {
				PreparedStatement preparedStmt = conn.prepareStatement(GetInvestigatorSQL);
				preparedStmt.setString(1, project__PROJECT_NUMBER);
				preparedStmt.setString(2, project__PROJECT_START_DATE);
				preparedStmt.setString(3, project__PROJECT_END_DATE);
				result = preparedStmt.executeQuery();
				result.next();
				String number = result.getString(1);
				continue;
			}
			catch (Exception ex) {;}

			project__LAST_UPDATE = dateFormat.format(current);
			DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
			project__DATE_ENTERED = dateFormatEnter.format(current);
			
			String[] output = {project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
					project__source_url,
					project__PROJECT_START_DATE,project__PROJECT_END_DATE,
					project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
					institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
					String.valueOf(institution_index__inst_id),
					agency_index__aid};
			csvout.writeNext(output);
		}
		csvout.close();
		webClient.close();
	}
}
