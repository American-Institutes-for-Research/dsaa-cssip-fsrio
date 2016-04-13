package com.jbt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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

import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;


public class Esrc {
	/**
	* This method calls scrape function in the Esrc class
	* 
	* @param url         The main web link associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	* @see               Run
	*/
	public static String esrcMain(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
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
		/**
		* This scraper goes through websites associated with ESRC
		* All major weblinks are specified in the config file (typically, process.cfg) and can be retrieved/updated there.
		*/
		Esrc.scrape(url,outfolder,conn,dbname, logfile);
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}		
		return "ESRC";

	}
	/**
	* This method scrapes the Esrc website.
	* 
	* @param url         The main web link associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the mainAHDB method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
*/
	
	public static void scrape(String url, String outfolder, Connection conn, String dbname, String logfile) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"ESRC_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", "project__source_url",
				"project__PROJECT_START_DATE", "project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
				"agency_index__aid",
				"investigator_data__name", "investigator_index__inv_id" };
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
		Elements links = doc.select("a");
		/**
		* Here is where we finished Part 1: identifying links to individual project pages.
		*/
		for (Element link : links) {
			/**
			* Every web scraper declares a list of variables that are present in project web pages. 
			* It is important that different websites can have different lists of data fields..
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
			String agency_index__aid = "81";
			String investigator_data__name = "";
			int investigator_index__inv_id = -1;
			int institution_index__inst_id = -2;
			String comment = "";

			/**
			 * Institution Variables
			 */
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_COUNTRY = "184";

			/**
			 * Processing Variables
			 */
			String piName = null;
			String piLastName = null;
			String piFirstName = null;
			/**
			* In this website, the list of project links can also have some grants. We pass them here
			*/
			if (!link.attr("href").startsWith("/grants")) {
				continue;
			}
			/**
			* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
			*/
			project__source_url = "http://researchcatalogue.esrc.ac.uk/"+link.attr("href");

			
			HtmlPage nextPage = webClient.getPage(project__source_url);
			Document finaldoc = Jsoup.parse(nextPage.asXml());
			
			project__PROJECT_TITLE = finaldoc.select("div[class=page-header]").text();
			project__PROJECT_NUMBER = link.attr("href").replace("/grants/", "").replace("/read", "");
			
			project__PROJECT_START_DATE = finaldoc.select("dt:contains(Start date) + dd").first().text();
			project__PROJECT_END_DATE = finaldoc.select("dt:contains(End date) + dd").first().text();
			/**
			* Extract just the year from project dates. 
			*/
			try {
				project__PROJECT_START_DATE = project__PROJECT_START_DATE.substring(project__PROJECT_START_DATE.length()-4);
				project__PROJECT_END_DATE = project__PROJECT_END_DATE.substring(project__PROJECT_END_DATE.length()-4);
			}
			catch (Exception e) {;}
			
			investigator_data__name = finaldoc.select("dt:contains(Grant Holder) + dd").first().text().toUpperCase();
			Pattern patToRem = Pattern.compile("^PROF |^PROFESSOR |^DR. |^DOCTOR |^DR |^MS |^MR |^MISS |^MRS ",Pattern.CASE_INSENSITIVE);
			Matcher matchToRem = patToRem.matcher(investigator_data__name);
			piName = matchToRem.replaceAll("").toLowerCase();
			piName = WordUtils.capitalizeFully(piName,' ','-');
			piLastName = piName.split(" ")[piName.split(" ").length-1];
			piFirstName = piName.replace(" "+piLastName, "");
			investigator_data__name = piLastName+", "+piFirstName;
		
			project__PROJECT_FUNDING = finaldoc.select("dt:contains(Grant amount) + dd").first().text().replace("\u00A3", "");
			project__PROJECT_OBJECTIVE =  finaldoc.select("div[class=col-sm-9]").select("p[class!=list-group-item-text][div[role!=tabpanel]],li[role!=presentation]").text();
			int in = project__PROJECT_OBJECTIVE.indexOf("Sort by:");
			if (in != -1)  project__PROJECT_OBJECTIVE = project__PROJECT_OBJECTIVE.substring(0,in);
			/**
			* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: project number, project start date, project end date, institution names and/or PI name (if applicable).
			* This is exactly what the following MySQL queries are doing.
			*/
			investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name);
			institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
		
			String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
			if (status.equals("Found")) continue;

			
			/**
			* Outputting all data into tab-separated file. 
			*/
			
			project__LAST_UPDATE = dateFormat.format(current);
			DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
			project__DATE_ENTERED = dateFormatEnter.format(current);
			
			String[] output = {project__PROJECT_NUMBER, project__PROJECT_TITLE, project__source_url,
					project__PROJECT_START_DATE, project__PROJECT_END_DATE, project__PROJECT_OBJECTIVE,
					agency_index__aid,
					investigator_data__name, String.valueOf(investigator_index__inv_id) };

			csvout.writeNext(output);
			
		}
		csvout.close();
		webClient.close();
	}

}
