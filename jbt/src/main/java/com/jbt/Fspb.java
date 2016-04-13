package com.jbt;
import net.sf.junidecode.Junidecode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLEncoder;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

public class Fspb {
	/**
	* This method calls scrape function in the FSPB class
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
	public static String fspbMain(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		/**
		* Opening one connection per scraper, as instructed. 
		*/
		Connection conn = MysqlConnect.connection(host,user,passwd);
		/**
		* The gargoylsoftware Web Client is rather capricious and prints out every JavaScript error possible even when they are meaningless for the scraper.
		* We have to shut down the default logger to make our customized one provide more meaningful messages.
		*/
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		/**
		* This scraper goes through websites associated with FSPB
		* All major weblinks are specified in the config file (typically, process.cfg) and can be retrieved/updated there.
		*/
		Fspb.scrape(url,outfolder,conn,dbname,logfile);
		return "FSPB";
	}
	/**
	* This method scrapes the FSPB website.
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"FSPB_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_data__INSTITUTION_ADDRESS1","institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_COUNTRY","institution_data__INSTITUTION_ZIP",
				"investigator_data__name",
				"institution_index__inst_id","investigator_index__inv_id",
				"agency_index__aid","comment"};
		
		csvout.writeNext(header);
		/**
		* The following code initiates the webClient and sets timeout at 50000 ms. 
		* Some websites, particularly Food Standards Agency are notorious for speed of response and require such high timeout value.
		*/
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		/**
		* Every web scraper consists of two parts:
		* 1) identify links to individual project pages
		* 2) scrape all necessary information from those individual project pages
		* <p>
		* The website structure is very different and it is impossible to template anything here from class extends and overrides.
		*/
		Elements projLinks = doc.select(".projects");
		
		/**
		* Here is where we finished Part 1: identifying links to individual project pages. We still have to filter out the invalid ones in the loop below.
		*/
		for (Element projLink : projLinks) {
			Document finaldoc = null;
			try {
				
				HtmlPage nextPage = webClient.getPage("http://www.safefood.eu/"+projLink.attr("href"));
				finaldoc = Jsoup.parse(nextPage.asXml());
				
			}
			catch (Exception htmlEx) {
				
					try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
						StringWriter errors = new StringWriter();
						htmlEx.printStackTrace(new PrintWriter(errors));
						out.println(currentDateLog
					    			+"   "
					    			+"Perhaps the link is broken or does not exist; it is also frequent "
					    			+ "at this particular website to have Unicode URLs that are not easily parsed - see at http://www.safefood.eu"
					    			+projLink.attr("href")+" ."
					    			+" and get project info manually if necessary. Here is some help with traceback:"
					    			+errors.toString());
					}catch (IOException e) {
	
					}
					continue;
				
			}
				
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
			String agency_index__aid = "82";
			int institution_index__inst_id = -1;
			int investigator_index__inv_id = -1;
			String comment = "";
			
			/**
			 * Institution Variables
			 */
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_ADDRESS1 = "";
			String institution_data__INSTITUTION_CITY = "";
			String institution_data__INSTITUTION_COUNTRY = "";
			String institution_data__INSTITUTION_ZIP = "";
			String institution_data__INSTITUTION_STATE = "";
			
			/**
			 * PI variables
			 */
			String investigator_data__name = "";
			
			/**
			 * Processing variables
			 */
			String piInfo = "";
			String piLastName = "";
			String piFirstName = "";
			String piName = "";
			
			/**
			* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
			*/
			project__source_url = "http://www.safefood.eu"+projLink.attr("href");
			
			/**
			 * Project Number
			 */
			try {
				project__PROJECT_NUMBER = finaldoc.select("h4:containsOwn(Project Reference)").first().nextElementSibling().text().replace("Project Reference:","");
			}
			catch (Exception exx) {
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
					StringWriter errors = new StringWriter();
					exx.printStackTrace(new PrintWriter(errors));
					out.println(currentDateLog
				    			+"   "
				    			+"Perhaps the link is broken or does not exist - see at http://www.safefood.eu"
				    			+projLink.attr("href")+" ."
				    			+" Here is some help with traceback:"
				    			+errors.toString());
				}catch (IOException e) {

				}
				continue;
			}
			
			
			/**
			 * Format date information was entered. 
			 */
			
			DateFormat dateFormatEntered = new SimpleDateFormat("yyyy-MM-dd");
			String currentEntered = dateFormatEntered.format(current);
			project__DATE_ENTERED = currentEntered;
			project__LAST_UPDATE = currentDateLog;
			
			/**
			 * Check to see if Project title exists, if it doesnt handle the exception and make title blank.
			 */
			try {
				project__PROJECT_TITLE = finaldoc.select("h3").text();
			}
			catch (Exception exx) {
				project__PROJECT_TITLE = "";
			}
			/**
			 *Extract project objective, and clean up.
			 */
			Elements projInfo = finaldoc.select("div.detail-page").last().children();
			for (int indElem=0;indElem<projInfo.size();indElem++) {
				Element nextSib = projInfo.get(indElem);
				if (nextSib.tagName() == "h4" && nextSib.text().startsWith("Abstract")) {
					for (int i=1; i<=5;i++) {
						if (projInfo.get(i+indElem).tagName() == "p") {
							project__PROJECT_OBJECTIVE += projInfo.get(i+indElem).text();
						} else {
							break;
						}
					}
				} 
				if (nextSib.tagName() == "h4" && nextSib.text().startsWith("Principal")) {
					for (int i=1; i<=5;i++) {
						if (projInfo.get(i+indElem).tagName() == "p") {
							piInfo += projInfo.get(i+indElem).text();
						} else {
							break;
						}
					}
				}
			}
			
			piInfo = Junidecode.unidecode(piInfo);
			
			/**
			 * Extract and format PI name. 
			 */
			piName = piInfo.split(", ")[0];
			Pattern patTitle = Pattern.compile("Mr\\.? |Dr\\.? |Ms\\.? |Mrs\\.? |Prof\\.? |Professor |Sir ");
			Matcher matchTitle = patTitle.matcher(piName);
			piName = matchTitle.replaceAll("");
			Pattern patFname = Pattern.compile("^(.*?)\\s+([A-Za-z']+)$");
			Matcher matcherFname = patFname.matcher(piName);
			while (matcherFname.find()) {
				piFirstName = matcherFname.group(1);
				piLastName = matcherFname.group(2);
			}
			investigator_data__name = piLastName+", "+piFirstName;
			
			
			/**
			 * Extract institution name and city
			 */
			if (piInfo.split(", ").length == 2) {
				institution_data__INSTITUTION_NAME = piInfo.split(", ")[1];
			} else if (piInfo.split(", ").length == 3) {
				institution_data__INSTITUTION_NAME = piInfo.split(", ")[1];
				institution_data__INSTITUTION_CITY = piInfo.split(", ")[2];
				String compCity = institution_data__INSTITUTION_CITY.replace("Co. ","");
				if (!compCity.equals(institution_data__INSTITUTION_CITY)) {
					institution_data__INSTITUTION_NAME+=", Co.";
					institution_data__INSTITUTION_CITY = compCity;
				} else {
					String compCity2 = institution_data__INSTITUTION_CITY.replace("Ltd. ","");
					if (!compCity2.equals(institution_data__INSTITUTION_CITY)) {
						institution_data__INSTITUTION_NAME+=", Ltd.";
						institution_data__INSTITUTION_CITY = compCity;
					}
				}
			} else if (piInfo.split(", ").length == 4) {
				institution_data__INSTITUTION_NAME = piInfo.split(", ")[1];
				institution_data__INSTITUTION_CITY = piInfo.split(", ")[3];
			} else if (piInfo.split(", ").length > 4) {
				institution_data__INSTITUTION_NAME = piInfo.split(", ")[1];
				institution_data__INSTITUTION_CITY = piInfo.split(", ")[2];
			} 
			
			/**
			 * Project Dates
			 */
			String projCommence = "";
			String projDura = "";
			try {
				projCommence = finaldoc.select("h4:containsOwn(Commencement Date)").first().nextElementSibling().text().replace("Commencement Date:","");
				projDura = finaldoc.select("h4:containsOwn(Project Duration)").first().nextElementSibling().text().replace("Project Duration:","");
			}
			catch (Exception exx) {
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
					StringWriter errors = new StringWriter();
					exx.printStackTrace(new PrintWriter(errors));
					out.println(currentDateLog
				    			+"   "
				    			+"Perhaps the project information is not complete yet and needs to be re-scraped later - pass it for now and come back later "
				    			+". Double check here if you wish - http://www.safefood.eu"
				    			+projLink.attr("href")+" ."
				    			+" Here is some help with traceback:"
				    			+errors.toString());
				}catch (IOException e) {

				}
				continue;
			}
			
			/**
			 * Convert duration into day counts
			 */
			int projDays = 0;
			if (projDura.split(" ")[1].startsWith("week")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*7;
			} else if (projDura.split(" ")[1].startsWith("month")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*30;
			} else if (projDura.split(" ")[1].startsWith("year")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*365;
			} else {
				/**
				 * Log an error with dates - projDura
				 */
			}
			/**
			* Extract just the year from project dates. 
			*/
			String startMonth = projCommence.split(" ")[0].replace(",","");
			project__PROJECT_START_DATE = projCommence.split(" ")[1].replace(",","");
			
			try {
				
				SimpleDateFormat inputFormat = new SimpleDateFormat("MMMM");
				Calendar cal = Calendar.getInstance();
				cal.setTime(inputFormat.parse(startMonth));
				SimpleDateFormat outputFormat = new SimpleDateFormat("MM"); // 01-12
				int daysToEndStartYear = (13-Integer.valueOf(outputFormat.format(cal.getTime())))*30;
				int daysAfterStartYear = projDays - daysToEndStartYear;
				if (daysAfterStartYear < 0) {
					project__PROJECT_END_DATE = project__PROJECT_START_DATE;
				} else {
					int ratioToYearLength = daysAfterStartYear / 365;
					project__PROJECT_END_DATE = String.valueOf(Integer.parseInt(project__PROJECT_START_DATE)+1+ratioToYearLength);
				}
				
			} catch (Exception eee) {
				
			}
			
			
			/**
			* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: project number, project start date, project end date, institution names and/or PI name (if applicable).
			* This is exactly what the following MySQL queries are doing.
			*/
			investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name);
			institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
		

			if (institution_index__inst_id == -1) {
				comment = "It is likely that the awardee institution of this project "
						+ "does not exist in institution data. Please follow the link "
						+ project__source_url
						+ "to look for additional information about the institution to be inserted into the database. "
						+ "The needed institution fields are empty in this row.";
			} 
			
			if (investigator_index__inv_id == -1) {
				if (!comment.equals("")) {
					comment = "It is likely that the Principal Contractor and awardee institution on this project "
						+ "do not exist in investigator data and institution data. Please follow the link "
						+ project__source_url
						+ " to look for additional information about the investigator to be inserted into the database. "
						+ "The needed investigator fields are empty in this row.";
				} else {
					comment = "It is likely that the Principal Contractor on this project "
							+ "does not exist in investigator data. Please follow the link "
							+ project__source_url
							+ " to look for additional information about the investigator to be inserted into the database. "
							+ "The needed investigator fields are empty in this row.";
				}
			} else {
				String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
				if (status.equals("Found")) continue;

			}
			
			/**
			* Outputting all data into tab-separated file. 
			* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
			*/
			String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
					project__source_url,
					project__PROJECT_START_DATE,project__PROJECT_END_DATE,
					project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
					institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
					institution_data__INSTITUTION_ADDRESS1,institution_data__INSTITUTION_CITY, 
					institution_data__INSTITUTION_COUNTRY,institution_data__INSTITUTION_ZIP,
					investigator_data__name.replaceAll("[\\n\\t\\r]"," "),
					String.valueOf(institution_index__inst_id),String.valueOf(investigator_index__inv_id),
					agency_index__aid,comment};
			
				csvout.writeNext(output);	
			
			}
		
		csvout.close();
		webClient.close();
	}
}

