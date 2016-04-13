package com.jbt;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

/**
* This class scrapes the website associated with Campden BRI.
* Returns String "CampdenBRI" when it done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
*/

public class CampdenBri {
	
	/**
	* This method calls the web scrapers associated with the Campden BRI website.
	* It loops through the links provided in the config file (typically, process.cfg).
	* There are two sets of links: main links are provided by FSRIO in the master spreadsheet. The additional links were discovered during exploration phase and added to the scraper.
	* 
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param links2      Additional web links associated with this scraper provided in the config file (typically, process.cfg). These are not currently used by FSRIO as in the master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* 
	* @return			 String "CampdenBRI" to signify that the scraper is done running.
	* @see               Run
	*/
	
	public static String campdenMain(String outfolder, String[] links, String[] links2, String host, String user, String passwd, String dbname) throws IOException {
		/**
		* The gargoylsoftware Web Client is rather capricious and prints out every JavaScript error possible even when they are meaningless for the scraper.
		* We have to shut down the default logger to make our customized one provide more meaningful messages.
		*/
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		
		/**
		* Opening one connection per scraper, as instructed. 
		*/
		Connection conn = MysqlConnect.connection(host,user,passwd);
		
		try {
		CampdenBri.scrapeV2(links,outfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The Campden BRI scraper with links not in FSRIO master spreadsheet did not succeed. This error has not been seen before. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}

		/**
		 *  These links2 are not part of current FSRIO approach but perhaps Campden BRI changed their website
		 *  and we can benefit from additional project info 
		 */
		try {
		CampdenBri.scrapeV1(links2,outfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The Campden BRI scraper with links provided in FSRIO master spreadsheet did not succeed. This error has not been seen before. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}

		MysqlConnect.closeConnection(conn);	
		return "CampdenBRI";
		
	}
	
	/**
	* This method runs the webscraper on Campden BRI main links provided by FSRIO.
	* 
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the campdenMain method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	*/

	
	public static void scrapeV1(String[] links, String outfolder, Connection conn, String dbname) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"CampdenBRI_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_OBJECTIVE",
				"investigator_data__EMAIL_ADDRESS",
				"investigator_data__name","investigator_data__PHONE_NUMBER",
				"institution_index__inst_id","investigator_index__inv_id",
				"agency_index__aid","investigator_data__INSTITUTION"};
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
		* This website has all information on individual projects within primary links and therefore part 1 is unnecessary.
		*/
		for (String link : links) {
			HtmlPage startPage = webClient.getPage(link);
			Document finaldoc = Jsoup.parse(startPage.asXml());
			/**
			 * Handling problems with UTF-8 encoding through scraper/browser settings
			 */
			finaldoc.outputSettings().charset().forName("UTF-8");
            finaldoc.outputSettings().escapeMode(EscapeMode.xhtml);
            
            Element content = finaldoc.select("main").first();
			content.select("br").remove(); 
            
			/**
			 * Retrieving information on individual projects from one page
			 */
			Elements projInfo = content.select("p");
			for (int i=0; i<projInfo.size();i++) {
				
				
				
				Element projElem = projInfo.get(i);
				
				Pattern patProj = Pattern.compile("Campden BRI [Pp]roject");
				Matcher matchProj = patProj.matcher(projElem.text());
				
				if (matchProj.find()) {
					/**
					* Every web scraper declares a list of variables that are present in project web pages. 
					* It is important that different websites can have different lists of data fields. That explains why we do not template, extend and override.
					*/
					String project__PROJECT_NUMBER = "";
					String project__PROJECT_TITLE = "";
					String project__PROJECT_START_DATE = "";
					String project__PROJECT_END_DATE = "";
					String project__PROJECT_OBJECTIVE = "";
					String investigator_data__PHONE_NUMBER = "";
					String agency_index__aid = "139";
					int institution_index__inst_id = 437;
					int investigator_data__INSTITUTION = 437;
					int investigator_index__inv_id = -1;
					String investigator_data__name = "";
					String investigator_data__EMAIL_ADDRESS = "";
					
					/**
					*Processing variables
					*/
					String piInfo= "";
					String piLastName = "";
					String piFirstName = "";
					
					/**
					 * Project title
					 */
					try {
						project__PROJECT_TITLE = projElem.previousElementSibling().text();
					} catch (Exception eee) {
						project__PROJECT_TITLE = "";
					}
					
					/**
					 * Project number and date
					 */
					List<String> matches = new ArrayList<String>();
					Pattern numdate = Pattern.compile("\\d+");
					Matcher matchnumdate = numdate.matcher(projElem.text());
					while (matchnumdate.find()) {
						matches.add(matchnumdate.group());
					}
					String[] allMatches = new String[matches.size()];
					allMatches = matches.toArray(allMatches);
					if (matches.size() == 3) {
						project__PROJECT_NUMBER = allMatches[0];
						project__PROJECT_START_DATE = allMatches[1];
						project__PROJECT_END_DATE = allMatches[2];
					} else if (matches.size() == 2) {
						project__PROJECT_NUMBER = "tbc";
						project__PROJECT_START_DATE = allMatches[0];
						project__PROJECT_END_DATE = allMatches[1];
					} else {
						project__PROJECT_NUMBER = allMatches[0];
					}

					/**
					 * Project objective and PI info
					 */
					int underSize;
					if (i+5 > projInfo.size()) {
						underSize = i+(projInfo.size()-i);
					} else {
						underSize = i+5;
					}
					for (int indElem=i+1;indElem<underSize;indElem++) {
						Element nextSib = projInfo.get(indElem);
						if (nextSib.attr("class") == "") {
							project__PROJECT_OBJECTIVE += nextSib.text() + " ";
						} else {
							/**
							 * PI info
							 */
							piInfo = nextSib.select("strong").text();
							piLastName = piInfo.split(" ")[piInfo.split(" ").length-1];
							Pattern patFname = Pattern.compile("^(.*?)\\s+\\w+$");
							Matcher matcherFname = patFname.matcher(piInfo.replace("Dr. ", ""));
							while (matcherFname.find()) {
								piFirstName = matcherFname.group(1);
							}
							investigator_data__name = piLastName+", "+piFirstName;
							String email_phone = nextSib.select("a").text();
							Pattern patEmailPhone = Pattern.compile("(^.*?)\\s([A-Za-z].*$)");
							Matcher matcherEmailPhone = patEmailPhone.matcher(email_phone);
							while (matcherEmailPhone.find()) {
								investigator_data__PHONE_NUMBER = matcherEmailPhone.group(1);
								investigator_data__EMAIL_ADDRESS = matcherEmailPhone.group(2);
							}
							break;
						}
					}
				
			
			if (project__PROJECT_NUMBER != "tbc") {
				
				/**
				* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
				* project number, project start date, project end date, institution names and/or PI name (if applicable).
				* This is exactly what the following MySQL queries are doing.
				* 
				* Institution ID is known in this particular source.
				*/
				investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname,investigator_index__inv_id,conn,investigator_data__name);
				String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
				if (status.equals("Found")) continue;
				
				/**
				* Outputting all data into tab-separated file. 
				* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
				* Will be one institution per line.
				*/
				String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
						project__PROJECT_START_DATE,project__PROJECT_END_DATE,
						project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
						investigator_data__EMAIL_ADDRESS,
						investigator_data__name.replaceAll("[\\n\\t\\r]"," "),investigator_data__PHONE_NUMBER,
						String.valueOf(institution_index__inst_id),String.valueOf(investigator_index__inv_id),
						agency_index__aid,String.valueOf(investigator_data__INSTITUTION)};
				
					csvout.writeNext(output);	
				
				}
			}
			
		}
		}
		webClient.close();
		csvout.close();
		
	}
	
	/**
	* This method runs the webscraper on Campden BRI additional links discovered during data exploration.
	* 
	* @param links       Additional web links associated with this scraper provided in the config file (typically, process.cfg). These are not currently used by FSRIO as in the master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the campdenMain method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	*/
	public static void scrapeV2(String[] links, String outfolder, Connection conn, String dbname) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"CampdenBRI_V2_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_OBJECTIVE",
				"investigator_data__EMAIL_ADDRESS",
				"investigator_data__name","investigator_data__PHONE_NUMBER",
				"institution_index__inst_id","investigator_index__inv_id",
				"agency_index__aid","investigator_data__INSTITUTION"};
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
		* This website has all information on individual projects within primary links and therefore part 1 is unnecessary.
		*/
		for (String link : links) {
			HtmlPage startPage = webClient.getPage(link);
			Document finaldoc = Jsoup.parse(startPage.asXml());
			/**
			 * Handling problems with UTF-8 encoding through scraper/browser settings
			 */
			((org.jsoup.nodes.Document) finaldoc).outputSettings().charset().forName("UTF-8");
            ((org.jsoup.nodes.Document) finaldoc).outputSettings().escapeMode(EscapeMode.xhtml);
            
            Element content = finaldoc.select("div.main_box").first();
			content.select("br").remove(); 
            
			/**
			 * Retrieving information on individual projects from one page
			 */
			Elements projInfo = content.select("p");
			for (int i=0; i<projInfo.size();i++) {
				
				
				
				Element projElem = projInfo.get(i);
				
				Pattern patProj = Pattern.compile("Campden BRI [Pp]roject");
				Matcher matchProj = patProj.matcher(projElem.text());

				if (matchProj.find()) {
					/**
					* Every web scraper declares a list of variables that are present in project web pages. 
					* It is important that different websites can have different lists of data fields. That explains why we do not template, extend and override.
					*/
					String project__PROJECT_NUMBER = "";
					String project__PROJECT_TITLE = "";
					String project__PROJECT_START_DATE = "";
					String project__PROJECT_END_DATE = "";
					String project__PROJECT_OBJECTIVE = "";
					String investigator_data__PHONE_NUMBER = "";
					String agency_index__aid = "139";
					int institution_index__inst_id = 437;
					int investigator_data__INSTITUTION = 437;
					int investigator_index__inv_id = -1;
					String investigator_data__name = "";
					String investigator_data__EMAIL_ADDRESS = "";
					
					/**
					*Processing variables
					*/
					String piInfo= "";
					String piLastName = "";
					String piFirstName = "";
					
					/**
					 * Project title
					 */
					project__PROJECT_TITLE = projElem.select("strong").text();

					/**
					 * Project number and date
					 */
					List<String> matches = new ArrayList<String>();
					Pattern numdate = Pattern.compile("\\d+");
					Matcher matchnumdate = numdate.matcher(projElem.text());
					while (matchnumdate.find()) {
						matches.add(matchnumdate.group());
					}
					String[] allMatches = new String[matches.size()];
					allMatches = matches.toArray(allMatches);
					if (matches.size() == 3) {
						project__PROJECT_NUMBER = allMatches[0];
						project__PROJECT_START_DATE = allMatches[1];
						project__PROJECT_END_DATE = allMatches[2];
					} else if (matches.size() == 2) {
						project__PROJECT_NUMBER = "tbc";
						project__PROJECT_START_DATE = allMatches[0];
						project__PROJECT_END_DATE = allMatches[1];
					} else if (matches.size() == 4) { 
						project__PROJECT_NUMBER = allMatches[1];
						project__PROJECT_START_DATE = allMatches[2];
						project__PROJECT_END_DATE = allMatches[3];
					} else {
						project__PROJECT_NUMBER = allMatches[0];
					}

					/**
					 * Project objective and PI info
					 */
					int underSize;
					if (i+5 > projInfo.size()) {
						underSize = i+(projInfo.size()-i);
					} else {
						underSize = i+5;
					}
					for (int indElem=i+1;indElem<underSize;indElem++) {
						Element nextSib = projInfo.get(indElem);
						Pattern patSib = Pattern.compile("Contact:");
						Matcher matchSib = patSib.matcher(nextSib.text());
						if (matchSib.find()) {
							/**
							 * PI info
							 */
							piInfo = nextSib.select("strong").text();
							piLastName = piInfo.split(" ")[piInfo.split(" ").length-1];
							Pattern patFname = Pattern.compile("^(.*?)\\s+\\w+$");
							Matcher matcherFname = patFname.matcher(piInfo.replace("Dr. ", ""));
							while (matcherFname.find()) {
								piFirstName = matcherFname.group(1);
							}
							investigator_data__name = piLastName+", "+piFirstName;
							investigator_data__EMAIL_ADDRESS = nextSib.select("a").text();
							nextSib.select("strong").remove();
							nextSib.select("a").remove();
							investigator_data__PHONE_NUMBER = nextSib.text().replace("Contact: ","").replace("+","").replace(" e-mail:","");
							break;

						} else {
							project__PROJECT_OBJECTIVE += nextSib.text() + " ";
						}
					}
				
			
			if (project__PROJECT_NUMBER != "tbc") {
				/**
				* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
				* project number, project start date, project end date, institution names and/or PI name (if applicable).
				* This is exactly what the following MySQL queries are doing.
				* 
				* Institution ID is known in this particular source.
				*/

				investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname,investigator_index__inv_id,conn,investigator_data__name);
				String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
				if (status.equals("Found")) continue;
				
				/**
				* Outputting all data into tab-separated file. 
				* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
				* Will be one institution per line.
				*/
				String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
						project__PROJECT_START_DATE,project__PROJECT_END_DATE,
						project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
						investigator_data__EMAIL_ADDRESS.replaceAll("[\\n\\t\\r]"," "),
						investigator_data__name.replaceAll("[\\n\\t\\r]"," "),investigator_data__PHONE_NUMBER,
						String.valueOf(institution_index__inst_id),String.valueOf(investigator_index__inv_id),
						agency_index__aid,String.valueOf(investigator_data__INSTITUTION)};
				
					csvout.writeNext(output);	
				
				
				}
			}
		}
		}
		webClient.close();
		csvout.close();
		
	}
}
