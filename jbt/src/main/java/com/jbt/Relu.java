package com.jbt;

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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

/**
* This class scrapes the website associated with the Rural Economy and Land Use Programme (RELU).
* Returns String "RELU" when done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
*/

public class Relu {
	/**
	* This method calls the web scraper associated with the Rural Economy and Land Use Programme (RELU).
	* It typically has several main links to retrieve all further information from and provided in the config file (typically, process.cfg).
	* 
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	*
	* @return			 String "RELU" to signify that the scraper is done running.
	* @see               Run
	*/
	public static String reluMain(String[] links, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
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
			Relu.scrape(links,outfolder,conn, dbname,logfile);
		} catch (Exception ex) {
			System.out.println("Warning: The RELU scraper did not succeed. This website is notorious for the downtime. "
					+ "Please check manually whether the project pages can be reached, e.g. starting at http://www.relu.ac.uk/research/Animal%20and%20Plant%20Disease/Animal%20and%20plant%20disease%20projects.html. "
					+ "If the website seems working and individual project pages are reachable, please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		MysqlConnect.closeConnection(conn);
		return "RELU";
	}
	
	/**
	* This method runs the webscraper on the RELU website.
	* 
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the web pages for data on individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the defraMain method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	*/
	public static void scrape(String[] links, String outfolder, Connection conn, String dbname, String logfile) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"RELU_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"investigator_data__name",
				"institution_index__inst_id","investigator_index__inv_id",
				"agency_index__aid"};
		
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
		for (String link : links) {
			
			HtmlPage startPage = webClient.getPage(link);
			Document doc = Jsoup.parse(startPage.asXml());
			
			/**
			* Here is where we finished Part 1: identifying links to individual project pages.
			*/
			Elements projLinks = doc.select("a[href*=relu.data-archive.ac.uk/explore-data/search-browse/project]");
            for (Element projLink : projLinks) {
            	
            	HtmlPage nextPage = webClient.getPage(projLink.attr("href"));
				Document finaldoc = Jsoup.parse(nextPage.asXml());
				
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
				String institution_data__INSTITUTION_NAME = "";
				String institution_data__INSTITUTION_CITY = "";
				String institution_data__INSTITUTION_COUNTRY = "";
				String institution_data__INSTITUTION_ZIP = "";
				String institution_data__INSTITUTION_ADDRESS1 = "";
				String institution_data__INSTITUTION_URL = "";
				String agency_index__aid = "87";
				int institution_index__inst_id = -1;
				int investigator_data__INSTITUTION = -1;
				int investigator_index__inv_id = -1;
				String investigator_data__name = "";
				
				/**
				 * Processing variables
				 */
				String piInfo= "";
				String instInfo = "";
				String piLastName = "";
				String piFirstName = "";
				
				try {
										
					/**
					* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
					*/
					project__source_url = projLink.attr("href");
					
					/**
					 * Project number
					 */
					project__PROJECT_NUMBER = finaldoc.select("th:containsOwn(Award:)").first().nextElementSibling().text();
					
					/**
					 * Project title
					 */
					project__PROJECT_TITLE = projLink.text();
					
					/**
					 * Project start and end dates
					 */
					String dates = finaldoc.select("th:containsOwn(Dates:)").first().nextElementSibling().text();
					project__PROJECT_START_DATE = dates.split(" - ")[0].split("/")[dates.split(" - ")[0].split("/").length-1];
					project__PROJECT_END_DATE = dates.split(" - ")[1].split("/")[dates.split(" - ")[1].split("/").length-1];
					
					/**
					 * Project objective
					 */
					project__PROJECT_OBJECTIVE = finaldoc.select("th:containsOwn(Dates:)").first().parent().nextElementSibling().nextElementSibling().text();
					
					/**
					 * PI and institution name
					 */
					instInfo = finaldoc.select("th:containsOwn(PI:)").first().nextElementSibling().text();
					piInfo = instInfo.split(", ")[0];
					Pattern patUniv = Pattern.compile("University|College");
					Matcher matchUniv = patUniv.matcher(instInfo.split(", ")[instInfo.split(", ").length-1]);
					Pattern patCityLast = Pattern.compile("North Wyke|Aberdeen|Wallingford|Knoxville|York|Lancaster");
					Matcher matchCityLast = patCityLast.matcher(instInfo.split(", ")[instInfo.split(", ").length-1]);
					if (matchCityLast.find() && !matchUniv.find()) {
						institution_data__INSTITUTION_NAME = instInfo.split(", ")[instInfo.split(", ").length-2];
						institution_data__INSTITUTION_CITY = instInfo.split(", ")[instInfo.split(", ").length-1];
					}
					else if (matchUniv.find()) {
						institution_data__INSTITUTION_NAME = instInfo.split(", ")[instInfo.split(", ").length-1];
					}
					else if (instInfo.split(", ").length > 3) {
						institution_data__INSTITUTION_NAME = instInfo.split(", ")[instInfo.split(", ").length-2];
						institution_data__INSTITUTION_CITY = instInfo.split(", ")[instInfo.split(", ").length-1];
						if (institution_data__INSTITUTION_NAME.equals("Canberra")) {
							institution_data__INSTITUTION_NAME = instInfo.split(", ")[instInfo.split(", ").length-3];
							institution_data__INSTITUTION_CITY = instInfo.split(", ")[instInfo.split(", ").length-2];
						}
					} else {
						institution_data__INSTITUTION_NAME = instInfo.split(", ")[instInfo.split(", ").length-1];
					}
					
					/**
					* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
					* project number, project start date, project end date, institution names and/or PI name (if applicable).
					* This is exactly what the following MySQL queries are doing.
					*/
					institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
					investigator_data__INSTITUTION = institution_index__inst_id;

					/**
					 * There can be several PIs - so, iterate through all and create separate rows for each
					 */
					for (String piOne : piInfo.split(" and ")) {
						piLastName = piOne.split(" ")[piOne.split(" ").length-1];
						Pattern patFname = Pattern.compile("^(.*?)\\s+[\\w-]+$");
						Matcher matcherFname = patFname.matcher(piOne.replace("Dr. ", ""));
						if (matcherFname.find()) {
							piFirstName = matcherFname.group(1);
						}
						investigator_data__name = piLastName+", "+piFirstName;
						investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name);
						
						String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
						if (status.equals("Found")) continue;
												
						/**
						* Outputting all data into tab-separated file. 
						* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
						* Will be one institution per line.
						*/
						String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
								project__source_url,
								project__PROJECT_START_DATE,project__PROJECT_END_DATE,
								project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
								institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
								investigator_data__name.replaceAll("[\\n\\t\\r]"," "),
								String.valueOf(institution_index__inst_id),String.valueOf(investigator_index__inv_id),
								agency_index__aid};
						
							csvout.writeNext(output);
						
					}
					
				}
				catch (Exception eee) {
					/**
					 * Log the link and error
					 */
					try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
						StringWriter errors = new StringWriter();
						eee.printStackTrace(new PrintWriter(errors));
						out.println(currentDateLog
					    			+"   "
					    			+"Perhaps the link is broken or does not exist, e.g. Page Not Found. It has been seen on this website before. The website can be broken altogether because it happens on RELU - "+projLink.attr("href")+" ."
					    			+" Here is some help with traceback:"
					    			+errors.toString());
					}catch (IOException e) {

					}
				}
            }
		}
		csvout.close();
		webClient.close();
	}

}
