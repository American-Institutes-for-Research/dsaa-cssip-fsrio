package com.jbt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

/**
* This class scrapes the website associated with Campden BRI.
* Returns String "DEFRA" when done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
*/

public class Defra {
	
	/**
	* This method calls the web scraper associated with the UK Department for Environment, Food and Rural Affairs (DEFRA).
	* It typically has only one main link to retrieve all further information from and provided in the config file (typically, process.cfg).
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* 
	* @return			 String "DEFRA" to signify that the scraper is done running.
	* @see               Run
	*/
	public static String defraMain(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
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
			Defra.scrape(url,outfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The DEFRA scraper did not succeed. This error has not been seen before. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		MysqlConnect.closeConnection(conn);
		return "DEFRA";

	}
	
	/**
	* This method runs the webscraper on the DEFRA website.
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the mainAHDB method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	*/
	public static void scrape(String url, String outfolder, Connection conn, String dbname) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"Defra_"+currentStamp+".csv"),'\t');
		
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME","institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ADDRESS1","institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_ZIP",
				"institution_index__inst_id",
				"agency_index__aid","comment"};
		csvout.writeNext(header);
		
		/**
		* The following code initiates the webClient and sets timeout at 50000 ms. 
		* Some websites, particularly Food Standards Agency are notorious for speed of response and require such high timeout value.
		*/
		WebClient webClient = new WebClient();
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		/**
		* Every web scraper consists of two parts:
		* 1) identify links to individual project pages
		* 2) scrape all necessary information from those individual project pages
		* <p>
		* This website has all information on individual projects within primary links and therefore part 1 is unnecessary.
		*/
		HtmlPage startPage = webClient.getPage(url);
		HtmlAnchor allResultsLink = startPage.getAnchorByText("View whole list");
		startPage = allResultsLink.click();
		Document doc = Jsoup.parse(startPage.asXml());
		/**
		* Here is where we finished Part 1: identifying links to individual project pages.
		*/
		Elements links = doc.select("a:containsOwn(Description)");
		for (Element link : links) {
			/**
			* Every web scraper declares a list of variables that are present in project web pages. 
			* It is important that different websites can have different lists of data fields. That explains why we do not template, extend and override.
			*/
			String project__PROJECT_NUMBER = "";
			String project__PROJECT_TITLE = "";
			String project__source_url = "";
			String project__PROJECT_START_DATE = "";
			String project__PROJECT_END_DATE = "";
			String project__PROJECT_MORE_INFO = "";
			String project__PROJECT_OBJECTIVE = "";
			String project__PROJECT_FUNDING = "";
			String agency_index__aid = "80";
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_COUNTRY = "184";
			String institution_data__INSTITUTION_ADDRESS1 = "";
			String institution_data__INSTITUTION_CITY = "";
			String institution_data__INSTITUTION_ZIP = "";
			int institution_index__inst_id = -1;
			int investigator_index__inv_id = -2;
			String comment = "";
			
			
			/**
			* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
			*/
			project__source_url = "http://randd.defra.gov.uk/"+link.attr("href");
			
			HtmlPage nextPage = webClient.getPage(project__source_url);
			Document finaldoc = Jsoup.parse(nextPage.asXml());
			
			finaldoc.select("br").remove();
			
			/**
			 * Project number
			 */
			String titleNum = finaldoc.select("h3").last().text();
			project__PROJECT_NUMBER = titleNum.split(" - ")[titleNum.split(" - ").length-1];
			
			/**
			 * Project title
			 */
			project__PROJECT_TITLE = titleNum.replace(" - "+project__PROJECT_NUMBER, "");
		
			/**
			 * Project more info
			 */
			project__PROJECT_MORE_INFO = finaldoc.select("h5:containsOwn(Description)").first().parent().text().replace("Description ","");
			
			try {
				/**
				 * Project objective
				 */
				project__PROJECT_OBJECTIVE = finaldoc.select("h5:containsOwn(Objective)").first().parent().text().replace("Objective ","");
			}
			catch (Exception e) {
				/**
				 * No objective - perhaps just pass
				 */
			}
			
			/**
			 *  Start and end dates
			 */
			project__PROJECT_START_DATE = finaldoc.select("b:containsOwn(From:)").first().parent().text().replace("From: ","");
			project__PROJECT_END_DATE = finaldoc.select("b:containsOwn(To:)").first().parent().text().replace("To: ","");
			

			
			/**
			 * Project funding
			 */
			project__PROJECT_FUNDING = finaldoc.select("b:containsOwn(Cost:)").first().parent().text().replace("Cost: ","").replace(",","").substring(1);
			
			/**
			 * Institution name and URL
			 */
			Element instTab = finaldoc.select("h5:containsOwn(Contractor / Funded Organisations)").first().nextElementSibling();
			institution_data__INSTITUTION_NAME = instTab.text();
			
			/**
			* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
			* project number, project start date, project end date, institution names and/or PI name (if applicable).
			* This is exactly what the following MySQL queries are doing.
			* 
			* There is no investigator info here.
			* Institution names are ambiguous - handling some of these issues on the fly here.
			*/
			
			institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
			if (institution_index__inst_id == -1) {
				Pattern patInst = Pattern.compile("\\((.*?)\\)");
				Matcher matchInst = patInst.matcher(institution_data__INSTITUTION_NAME);
				if (matchInst.find()) {
					/**
					 * Check institution in MySQL DB (might be the one in parentheses)
					 */
					institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, matchInst.group(1));
					/**
					 * Might be the one in parentheses but split by dash
					 */
					if (institution_index__inst_id == -1) {
						try {
							institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, matchInst.group(1).split(" - ")[0]);
						} catch (Exception ee) {
							/**
							 * No need to handle exception because it is just trying various possible patterns to match inst name with the DB.
							 */
						}
					}
				} else {
					/**
					 * Check institution in MySQL DB (might be the one after dash)
					 */
					try {
						institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME.split(" - ")[1]);
					} catch (Exception ee) {
						/**
						 * No need to handle exception because it is just trying various possible patterns to match inst name with the DB.
						 */
					}
				}
			}
			
			if (institution_index__inst_id == -1) {
				comment = "Please populate institution fields by exploring the institution named on the project.";	
			}

			String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
			if (status.equals("Found")) continue;


			/**
			* Outputting all data into tab-separated file. 
			* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
			*/
			String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),project__source_url,
					project__PROJECT_START_DATE,project__PROJECT_END_DATE,
					project__PROJECT_MORE_INFO.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
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
