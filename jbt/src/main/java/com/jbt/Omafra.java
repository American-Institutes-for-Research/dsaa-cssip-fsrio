package com.jbt;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
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
* This class scrapes the website associated with the Ontario Ministry of Agriculture, Food and Rural Affairs (OMAFRA).
* Returns String "OMAFRA" when done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
*/

public class Omafra {
	/**
	* This method calls the web scraper associated with the UK Department for Environment, Food and Rural Affairs (DEFRA).
	* It typically has only one main link to retrieve all further information from and provided in the config file (typically, process.cfg).
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* 
	* @return			 String "OMAFRA" to signify that the scraper is done running.
	* @see               Run
	*/
	public static String omafraMain(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
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
		 * All links to individual projects on this website follow a given pattern in their 'href' property.
		 */
		String pat = "english/research/foodsafety/\\d+";
		try {	
			Omafra.scrape(url,pat,outfolder,conn,dbname);
		} catch (Exception ex) {
			System.out.println("Warning: The OMAFRA scraper did not succeed. This error has not been seen before, i.e. not handled separately. It is possible that the website is down. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		MysqlConnect.closeConnection(conn);
		return "OMAFRA";
	}
	/**
	* This method runs the webscraper on the OMAFRA website.
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param pat		 Regular expressions passed from omafraMain method to match all individual project web page links by their 'href' property.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the mainAHDB method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	*/
	public static void scrape(String url, String pat, String outfolder, Connection conn, String dbname) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"Omafra_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
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
		/**
		* Every web scraper consists of two parts:
		* 1) identify links to individual project pages
		* 2) scrape all necessary information from those individual project pages
		* <p>
		* This website has all information on individual projects within primary links and therefore part 1 is unnecessary.
		*/
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
				
		Elements links = doc.select("a[href]");
		Pattern pattern = 
	            Pattern.compile(pat);
		
		for (Element link : links) {
			Matcher matcher = 
		            pattern.matcher(link.attr("href"));

			if (matcher.find())	{
				HtmlPage nextPage = webClient.getPage("http://www.omafra.gov.on.ca/"+link.attr("href"));
				Document linkdoc = Jsoup.parse(nextPage.asXml());
				
				Elements linksInside = linkdoc.select("a[href]");
				for (Element linkInside : linksInside) {
					Matcher matcherLinks = 
				            pattern.matcher(linkInside.attr("href"));
					if (matcherLinks.find()) {
						/**
						* Here is where we finished Part 1: identifying links to individual project pages.
						*/
						HtmlPage finalPage = webClient.getPage("http://www.omafra.gov.on.ca/"+linkInside.attr("href"));
						Document finaldoc = Jsoup.parse(finalPage.asXml());

						Element content = finaldoc.getElementById("right_column");
						
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
						String project__PROJECT_ABSTRACT = "";
						String institution_data__INSTITUTION_NAME = "";
						String agency_index__aid = "69";
						int institution_index__inst_id = -1;
						int investigator_index__inv_id = -1;
						String comment = "";
						String investigator_data__name = "";
						
						/**
						 * Processing variables
						 */
						String instpidata = "";
						String piName = "";
						String instInfo = "";
						String piLastName = "";
						String piFirstName = "";
						
						/**
						* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
						*/
						project__source_url = "http://www.omafra.gov.on.ca/"+linkInside.attr("href");
						
						/**
						 * Project title and number
						 */
						try {
							Elements titleElem = content.getElementsByTag("h2");
							String title = titleElem.text();
							project__PROJECT_NUMBER = title.split(" - ")[0];
							project__PROJECT_TITLE = title.split(" - ")[1];
						}
						catch (Exception e) {;} 
						
						
						Element divElem = content.child(0);
						Elements allElem = divElem.children();
						for (Element elem : allElem) {
							/**
							 * Start and end date
							 */
							Pattern patdate = Pattern.compile("Start-End Date:\\s+(\\d+)-(\\d+)");
							Matcher matcherDate = patdate.matcher(elem.text());
							if (matcherDate.find()) {
								project__PROJECT_START_DATE = matcherDate.group(1);
								project__PROJECT_END_DATE = matcherDate.group(2);
							}
							
							/**
							 * Expected benefits (more info)
							 */
							Pattern patBenefits = Pattern.compile("Expected Benefits");
							Matcher matcherBenefits = patBenefits.matcher(elem.text());
							if (matcherBenefits.find()) {
								project__PROJECT_MORE_INFO = elem.nextElementSibling().text();
							}
							
							/**
							 * Project objectives
							 */
							Pattern patObjectives = Pattern.compile("Objectives");
							Matcher matcherObjectives = patObjectives.matcher(elem.text());
							if (matcherObjectives.find()) {
								project__PROJECT_OBJECTIVE = elem.nextElementSibling().text();
							}
							
							/**
							 * Abstract / description
							 */
							Pattern patAbstract = Pattern.compile("Description");
							Matcher matcherAbstract = patAbstract.matcher(elem.text());
							if (matcherAbstract.find()) {
								project__PROJECT_ABSTRACT = elem.nextElementSibling().text();
							}
							
							/**
							 * Institution and PI
							 */
							Pattern patInstPi = Pattern.compile("Lead researcher|Researcher");
							Matcher matcherInstPi = patInstPi.matcher(elem.text());
							if (matcherInstPi.find() && elem.tagName() == "h3") {
								instpidata = elem.nextElementSibling().text().split("; ")[0];
								String[] piInfo = instpidata.split(", ")[0].split(" ");
								Pattern patLname = Pattern.compile("\\((\\w+)\\)");
								Matcher matcherLname = patLname.matcher(piInfo[piInfo.length-1]);
								if (matcherLname.find()) {
									piLastName = matcherLname.group(1);
									Pattern patFname = Pattern.compile("^(.*?)\\s+");
									Matcher matcherFname = patFname.matcher(instpidata.split(", ")[0].replace("Dr. ", ""));
									while (matcherFname.find()) {
										piFirstName = matcherFname.group(1);
									}
								}
								else {
									piLastName = piInfo[piInfo.length-1];
									Pattern patFname = Pattern.compile("^(.*?)\\s+\\w+$");
									Matcher matcherFname = patFname.matcher(instpidata.split(", ")[0].replace("Dr. ", ""));
									while (matcherFname.find()) {
										piFirstName = matcherFname.group(1);
									}
								}
								piName = piLastName+", "+piFirstName;
								instInfo = instpidata.split(", ")[instpidata.split(", ").length-1];
								
							}
							if (instpidata == "") {
								if (elem.tagName() == "h2") {
									instpidata = elem.nextElementSibling().text().split("; ")[0];
									String[] piInfo = instpidata.split(", ")[0].split(" ");
									Pattern patLname = Pattern.compile("\\((\\w+)\\)");
									Matcher matcherLname = patLname.matcher(piInfo[piInfo.length-1]);
									if (matcherLname.find()) {
										piLastName = matcherLname.group(1);
										Pattern patFname = Pattern.compile("^(.*?)\\s+");
										Matcher matcherFname = patFname.matcher(instpidata.split(", ")[0].replace("Dr. ", ""));
										while (matcherFname.find()) {
											piFirstName = matcherFname.group(1);
										}
									}
									else {
										piLastName = piInfo[piInfo.length-1];
										Pattern patFname = Pattern.compile("^(.*?)\\s+\\w+$");
										Matcher matcherFname = patFname.matcher(instpidata.split(", ")[0].replace("Dr. ", ""));
										while (matcherFname.find()) {
											piFirstName = matcherFname.group(1);
										}
									}
									
									piName = piLastName+", "+piFirstName;
									instInfo = instpidata.split(", ")[instpidata.split(", ").length-1];
								}
							}
							
							
							
						}
						
						/**
						* Based on FSRIO guidance, we are checking on several fields whether the project exists in the DB: 
						* project number, project start date, project end date, institution names and/or PI name (if applicable).
						* This is exactly what the following MySQL queries are doing.
						*/
						investigator_data__name = piName;
						institution_data__INSTITUTION_NAME = instInfo;
						institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
						investigator_index__inv_id = MysqlConnect.GetInvestigatorSQL(dbname, investigator_index__inv_id, conn, investigator_data__name);
						String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
						if (status.equals("Found")) continue;

						if (institution_index__inst_id == -1) {
							comment = "It is likely that the awardee institution of this project "
									+ "does not exist in institution data. Please follow the link "
									+ project__source_url
									+ "to look for additional information about the institution to be inserted into the database. "
									+ "The needed institution fields are empty in this row.";
						} 
						
						if (investigator_index__inv_id == -1) {
							investigator_data__name = piName;
							comment = "It is likely that the Lead researcher on this project "
									+ "does not exist in investigator data. Please follow the link "
									+ project__source_url
									+ "to look for additional information about the investigator to be inserted into the database. "
									+ "The needed investigator fields are empty in this row.";
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
								investigator_data__name.replaceAll("[\\n\\t\\r]"," "),
								String.valueOf(institution_index__inst_id),String.valueOf(investigator_index__inv_id),
								agency_index__aid,comment};
						
							csvout.writeNext(output);	
						
					}
				}
				
			}
		}
		csvout.close();
		webClient.close();
		
	}

}
