package com.jbt;

/* fix start and end date for some projects */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
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

public class Fsa {
	/**
	* This method calls scrape function in the FSA class
	* 
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the websites where web pages for individual projects can be found.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	* @see               Run
	*/
	public static String fsaMain(String[] links, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
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
		* This scraper goes through websites associated with FSA
		* All major weblinks are specified in the config file (typically, process.cfg) and can be retrieved/updated there.
		*/
		Fsa.scrape(links,outfolder,conn,dbname,logfile);

		return "FSA";
	}
	/**
	* This method scrapes the FSA website.
	* 
	* @param links       The main web links associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The links are "entry points" into the websites where web pages for individual projects can be found.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the mainAHDB method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
*/
	public static void scrape(String[] links,String outfolder, Connection conn, String dbname, String logfile) throws IOException {
		/**
		* The date is needed in every subclass for logging and file naming purposes given that implement a customized logger for the most transparent and easiest error handling and troubleshooting.
		*/
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		/**
		* As seen, the naming convention for the output files is class, method, and current date. This is needed to impose version control and easier data organization for FSRIO staff.
		*/
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"FSA_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_index__inst_id",
				"agency_index__aid","comment"};
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
			Document doc = null;	
			try {
				
				HtmlPage startPage = webClient.getPage(link);
				doc = Jsoup.parse(startPage.asXml());
				
			}
			catch (Exception exx) {
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
					StringWriter errors = new StringWriter();
					exx.printStackTrace(new PrintWriter(errors));
					out.println(dateFormat.format(current)
				    			+"   "
				    			+"Perhaps the link is broken or does not exist; "
				    			+"it might also be a seminar or workshop that's not a project - see at "
				    			+link+" ."
				    			+" Here is some help with traceback:"
				    			+errors.toString());
				}catch (IOException e) {

				}
				continue;
			}
            Element content = doc.select("div.main-content-inner").first();
            Elements projLinks = content.select("a[href*=/science/research/]");
            for (Element projLink : projLinks) {
            	Pattern patSymb = Pattern.compile("#");
            	Matcher badSymb = patSymb.matcher(projLink.attr("href"));
            	if (!badSymb.find()) {
            		if (!Arrays.asList(links).contains("http://www.food.gov.uk"+projLink.attr("href"))) {
            			
            			try {
            				HtmlPage finalPage = webClient.getPage("http://www.food.gov.uk/"+projLink.attr("href"));
            				Document finaldoc = Jsoup.parse(finalPage.asXml());

	            			
            				/**
            				* Every web scraper declares a list of variables that are present in project web pages. 
            				* It is important that different websites can have different lists of data fields..
            				*/
            				String project__PROJECT_NUMBER = "";
	    					String project__PROJECT_TITLE = "";
	    					String project__source_url = "";
	    					String project__PROJECT_START_DATE = "";
	    					String project__PROJECT_END_DATE = "";
	    					String project__PROJECT_MORE_INFO = "";
	    					String project__PROJECT_OBJECTIVE = "";
	    					String project__LAST_UPDATE = "";
	    					String project__DATE_ENTERED = "";
	    					String agency_index__aid = "65";
            				int investigator_index__inv_id = -2;
            				int institution_index__inst_id = -2;
            				String comment = "";
	    					
            				/**
            				 * Institution Variables
            				 */
            				String institution_data__INSTITUTION_NAME = "";
								    					
							/**
							* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
							*/
	    					project__source_url = "http://www.food.gov.uk"+projLink.attr("href");
	    					
	    					/**
	    					 * Retrieve project info. Check multiple kinds..
	    					 */
	    					project__PROJECT_NUMBER = finaldoc.select("strong:containsOwn(Project code)").first().parent().text().replace("Project code: ","");
	    					String projInfo = "";
	    					if (project__PROJECT_NUMBER.contains("Study Duration")) {
	    						projInfo = finaldoc.select("strong:containsOwn(Project code)").first().parent().text();
	    						Pattern patNum = Pattern.compile("Project code\\:\\s+(.*?)");
	    						Matcher matchNum = patNum.matcher(projInfo);
	    						project__PROJECT_NUMBER = matchNum.group(1);
	    					}
	    					
	    					/**
	    					* Extract just the year from project dates. 
	    					*/
	        				if (projInfo.equals("")) {
	        					String dates = finaldoc.select("strong:containsOwn(Study duration)").first().parent().text().replace("Study duration: ","");
		    					Pattern patDates = Pattern.compile(".*?([0-9]{4}).*?([0-9]{4})");
	        					Matcher matchDates = patDates.matcher(dates);
	        					while (matchDates.find()) {
	        						project__PROJECT_START_DATE = matchDates.group(1);
	        						project__PROJECT_END_DATE = matchDates.group(2);
	        					}
	        				} else {
	        					Pattern patDates = Pattern.compile("Study Duration\\:\\s+([0-9]{4}).*?([0-9]{4})");
	        					Matcher matchDates = patDates.matcher(projInfo);
	        					while (matchDates.find()) {
	        						project__PROJECT_START_DATE = matchDates.group(1);
	        						project__PROJECT_END_DATE = matchDates.group(2);
	        					}
	        				}
	        				/**
	        				* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: project number, project start date, project end date, institution names and/or PI name (if applicable).
	        				* This is exactly what the following MySQL queries are doing.
	        				*/
	        				try {
	        					/** 
	        					* It is normal at this website not to have institution information and it is not critical. So, no need to distract the flow by meaningful exception handler.
	        					*/
	        					if (projInfo.equals("")) {
	        						institution_data__INSTITUTION_NAME = finaldoc.select("strong:containsOwn(Contractor)").first().parent().text().replace("Contractor: ","");
	        					} else {
	        						Pattern patInst = Pattern.compile("Contractor\\:\\s+(.*?)");
		        					Matcher matchInst = patInst.matcher(projInfo);
		        					while (matchInst.find()) {
		        						institution_data__INSTITUTION_NAME = matchInst.group(1);
		        					}
	        					}
	        					institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);

	        				} catch (Exception ee) {
	        					comment = "No institution information available; please check "+ project__source_url + " to identify if any additional information can be retrieved.";
	        				}
	        				
	        				String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
	        				if (status.equals("Found")) continue;


	    					
    						/**
    						 * Dates entered and updated
    						 */
        					DateFormat dateFormatEntered = new SimpleDateFormat("yyyy-MM-dd");
        					String currentEntered = dateFormatEntered.format(current);
        					project__DATE_ENTERED = currentEntered;
        					project__LAST_UPDATE = dateFormat.format(current);
        					
        					/**
        					 * Extract project title.
        					 */
        					project__PROJECT_TITLE = finaldoc.select("#page-title").text();

        					
        					/**
        					* Parsing project objective - hard to deal with bad structure 
        					*/
	        				try {
								project__PROJECT_OBJECTIVE = finaldoc.select("span:containsOwn(Background)").last().parent().nextElementSibling().text();
							} catch (Exception ee) {
								try {
									Element objDiv = finaldoc.select("h2:containsOwn(Background)").last().parent().parent().parent().nextElementSibling();
									objDiv.select("p").first().remove();
									project__PROJECT_OBJECTIVE = objDiv.text();
								} catch (Exception eee) {
									project__PROJECT_OBJECTIVE = finaldoc.select("span:containsOwn(Background)").get(1).parent().nextElementSibling().text();
								}
							}
							
        					/**
        					* Parsing project More info - hard to deal with bad structure 
        					*/							
	        				try {
								project__PROJECT_MORE_INFO = finaldoc.select("span:containsOwn(Research Approach)").last().parent().nextElementSibling().text();
							} catch (Exception ee) {
								try {
									Element objDiv = finaldoc.select("h2:containsOwn(Research Approach)").last().parent().parent().parent().nextElementSibling();
									project__PROJECT_MORE_INFO = objDiv.text();
								} catch (Exception eee) {
									project__PROJECT_MORE_INFO = finaldoc.select("span:containsOwn(Results)").last().parent().nextElementSibling().text();
								}
							}
							
	        				/**
	        				* Outputting all data into tab-separated file. 
	        				* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
	        				*/
	        				String[] output = {project__PROJECT_NUMBER.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_TITLE.replaceAll("[\\n\\t\\r]"," "),
									project__source_url,
									project__PROJECT_START_DATE,project__PROJECT_END_DATE,
									project__PROJECT_MORE_INFO.replaceAll("[\\n\\t\\r]"," "),project__PROJECT_OBJECTIVE.replaceAll("[\\n\\t\\r]"," "),
									institution_data__INSTITUTION_NAME.replaceAll("[\\n\\t\\r]"," "),
									String.valueOf(institution_index__inst_id),
									agency_index__aid,comment};
							
								csvout.writeNext(output);	
	    					
	            		} catch (Exception ex) {
	            			try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
								StringWriter errors = new StringWriter();
								Date currentLog = new Date();
								String currentDateLog = dateFormat.format(currentLog);
								
								ex.printStackTrace(new PrintWriter(errors));
								out.println(currentDateLog
							    			+"   "
							    			+"Very unlikely here that something is broken - "
							    			+"exception is just because it's not a project"
							    			+"but a project list or seminar or workshop that's not a project - "
							    			+"double check for the ease of your heart at http://www.food.gov.uk"
							    			+projLink.attr("href")+" ."
							    			+" Here is some help with traceback:"
							    			+errors.toString());
								
							}catch (IOException e) {

							}
	            			
	            		}
	            	}
            	}
            }
			
		}
		csvout.close();
		webClient.close();
	}
}
