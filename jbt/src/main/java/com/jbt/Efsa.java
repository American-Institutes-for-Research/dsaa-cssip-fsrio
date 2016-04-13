package com.jbt;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.StringWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.opencsv.CSVWriter;

/**
* This class scrapes the website associated with the European Food Safety Authority (EFSA).
* Returns String "EFSA" when done. The class writes output directly into the tab-separate spreadsheet for review and quality control.
* Utilizes the links provided by FSRIO and requires several parameters specified in the main Run class.
*/

public class Efsa {

	/**
	* This method calls the web scraper associated with the European Food Safety Authority (EFSA).
	* It typically has only one main link to retrieve all further information from and provided in the config file (typically, process.cfg).
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param host        Host name for the server where FSRIO Research Projects Database resides, e.g. "localhost:3306". Parameter is specified in config file. The port is 3306.
	* @param user        Username for the server where FSRIO Research Projects Database resides. Parameter is specified in config file.
	* @param passwd      Password for the server where FSRIO Research Projects Database resides. Parameter is passed through command line.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	*
	* @return			 String "EFSA" to signify that the scraper is done running.
	* @see               Run
	* @throws	         IOException
	*/
	public static String efsaMain(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
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
		String pat = "en/tender.*?/tender/|en/node/915681";
		
		try {
			Efsa.scrape(url,pat,outfolder,conn,dbname,logfile);
		} catch (Exception ex) {
			System.out.println("Warning: The EFSA scraper did not succeed. This error has not been seen before. Please share the following information with the IT support to troubleshoot:");
			ex.printStackTrace();
			System.out.println("It is recommended to re-run this data source at least once more to make sure that no system error is at fault, such as firewall settings or internet connection.");
		}
		MysqlConnect.closeConnection(conn);
		return "EFSA";
	}
	
	/**
	* This method runs the webscraper on the EFSA website.
	* 
	* @param url         The main URL associated with this scraper provided in the config file (typically, process.cfg) and retrieved from the FSRIO master spreadsheet. The link is the "entry point" into the web pages of individual projects.
	* @param pat		 Regular expressions passed from efsaMain method to match all individual project web page links by their 'href' property.
	* @param outfolder   The folder name specified in the config file (typically, process.cfg) where all output tab-separated files are written.
	* @param conn        Database connection initiated in the efsaMain method.
	* @param dbname      Name of the FSRIO Research Projects Database that is being updated. Parameter is specified in config file. It is needed in every individual scraper because dbname is specified in MySQL queries checking whether project exists in the DB.
	* @param logfile     Path to the log file where IT-related issues are written with meaningful messages. These errors are primarily to be reviewed by IT support rather than data entry experts. The latter group receives warning messages directly in the console.
	*/
	public static void scrape(String url, String pat, String outfolder, Connection conn, String dbname, String logfile) throws IOException {
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
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"EFSA_"+currentStamp+".csv"),'\t');
		/**
		* Different websites can provide different information on individual projects that is mapped to the FSRIO Research Projects Database.
		* The naming convention here is [table name]__[data_field]. It is important to keep this naming convention for the database upload process after the output QA is complete.
		*/
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE",
				"project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_data__INSTITUTION_ADDRESS1","institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_STATE","institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ZIP",
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
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		
		String sizeLinks = doc.select("div:containsOwn(Results 1)").text(); 
		Pattern searchSize = Pattern.compile("(\\d+)$");
		Matcher matchSize = searchSize.matcher(sizeLinks);
		int numPages = 0;
		while (matchSize.find()) {
			numPages = Integer.valueOf(matchSize.group(1))/20;
		}
		
		if (numPages == 0) {
			//Log an error that it didn't work
		} else {
		
			for (int i=0;i<=numPages;i++) {
				HtmlPage nextPage = webClient.getPage(url+"&page="+String.valueOf(i));
				Document listTenders = Jsoup.parse(nextPage.asXml());
				
				Elements links = listTenders.select("a[href]");
				Pattern pattern = 
			            Pattern.compile(pat);
				/**
				 * Check whether all links are being captured given pattern - should be 20 per page except for very last
				 */
				int checkNums = 0;
				
				for (Element link : links) {
					/**
					* In this website, the list of project links need to match a specific regular expression in their 'href' property.
					*/
					Matcher matcher = 
				            pattern.matcher(link.attr("href"));
					if (matcher.find()) {
						checkNums++;
						HtmlPage nextnextPage = webClient.getPage("http://efsa.europa.eu/"+link.attr("href"));
						Document linkdoc = Jsoup.parse(nextnextPage.asXml());
						Elements furtherLink = linkdoc.select("a:containsOwn(award notice)");
						Element infoLink = null;
						if (furtherLink.size()!=0) {
							infoLink = furtherLink.last();
						} else {
							try {
								furtherLink = linkdoc.select("a:containsOwn(Bekanntmachung)");
								infoLink = furtherLink.last();
							}
							catch (Exception ex) {
								/**
								 * There is simply no link here for individual project information. Need to ignore.
								 */
							}
						}
						if (infoLink != null) {
							try {
								/**
								* Here is where we finished Part 1: identifying links to individual project pages.
								*/
								HtmlPage finalPage = webClient.getPage(infoLink.attr("href"));
								Document finaldoc = Jsoup.parse(finalPage.asXml());
								
								Element content = finaldoc.getElementById("fullDocument");
								
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
								String agency_index__aid = "122";
								int institution_index__inst_id = -1;
								String institution_data__INSTITUTION_NAME = "";
								String institution_data__INSTITUTION_ADDRESS1 = "";
								String institution_data__INSTITUTION_CITY = "";
								String institution_data__INSTITUTION_COUNTRY = "";
								String institution_data__INSTITUTION_ZIP = "";
								String institution_data__INSTITUTION_STATE = "";
								int investigator_index__inv_id = -2;
								String comment = "";
								
								/**
								 * Processing variables
								 */
								String instInfo = "";
								
								/**
								* Very important field - project__source_url - is used in Warning notes and for logging to check back. It is critical during QA too.
								*/
								project__source_url = infoLink.attr("href");
								
								/**
								 * Project number
								 */
								Element numElem = content.select("span:containsOwn(File reference number attributed)").first();
								Pattern badSymb = Pattern.compile("^[\\W_]+");
								project__PROJECT_NUMBER = numElem.nextElementSibling().text();
								Matcher matchSymb = badSymb.matcher(project__PROJECT_NUMBER);
								project__PROJECT_NUMBER = matchSymb.replaceAll("");
								Pattern patNum = Pattern.compile("^(.*?)\\s");
								Matcher matchNum = patNum.matcher(project__PROJECT_NUMBER);
								if (matchNum.find()) {
									project__PROJECT_NUMBER = matchNum.group(1);
								} else {
									Pattern badSymbEnd = Pattern.compile("\\.$");
									Matcher matchSymbEnd = badSymbEnd.matcher(project__PROJECT_NUMBER);
									project__PROJECT_NUMBER = matchSymbEnd.replaceAll("");
								}
								
								/**
								 * Project start date. End date is not available on this source.
								 */
								Element dateElem = content.select("span:containsOwn(Date of contract award)").first().nextElementSibling();
								String startDate = dateElem.text();
								Pattern patDate = Pattern.compile("(\\d+)$");
								Matcher matchDate = patDate.matcher(startDate);
								while (matchDate.find()) {
									project__PROJECT_START_DATE = matchDate.group(1);
								}
								
								/**
								 * Title
								 */
								Element titleElem = content.select("span:containsOwn(Title attributed to)").first();
								project__PROJECT_TITLE = titleElem.nextElementSibling().text();
								project__PROJECT_TITLE = project__PROJECT_TITLE.replace(project__PROJECT_NUMBER,"");
								matchSymb = badSymb.matcher(project__PROJECT_TITLE);
								project__PROJECT_TITLE = matchSymb.replaceAll("");
								
								/**
								 * Project abstract
								 */
								Element abstElem = content.select("span:containsOwn(Short description of)").first();
								project__PROJECT_OBJECTIVE = abstElem.nextElementSibling().text();
								matchSymb = badSymb.matcher(project__PROJECT_OBJECTIVE);
								project__PROJECT_OBJECTIVE = matchSymb.replaceAll("");
								
								/**
								 * Institution info - can be several if multiple contracts awarded under one tender
								 */
								Elements instElems = content.select("span:containsOwn(Name and address of economic operator)");
								for (Element instElem : instElems) {
									/**
									 * Not all institutions have every variable below - so, we need to make all of them empty at start of the loop.
									 */
									institution_data__INSTITUTION_NAME = "";
									institution_data__INSTITUTION_ADDRESS1 = "";
									institution_data__INSTITUTION_CITY = "";
									institution_data__INSTITUTION_COUNTRY = "";
									institution_data__INSTITUTION_ZIP = "";
									institution_data__INSTITUTION_STATE = "";
									institution_index__inst_id = -1;
									comment = "";
									
									Element instContainer = instElem.nextElementSibling();
									instInfo = StringEscapeUtils.unescapeHtml4(instContainer.children().select("p").html().toString());
									List<String> matches = new ArrayList<String>();
									for (String instInfoElem : instInfo.split("<br>")) {
										matches.add(instInfoElem);
									}
									
									String[] allMatches = new String[matches.size()];
									allMatches = matches.toArray(allMatches);
									institution_data__INSTITUTION_NAME = allMatches[0];
									
									institution_index__inst_id = MysqlConnect.GetInstitutionSQL(dbname, institution_index__inst_id, conn, institution_data__INSTITUTION_NAME);
									if (institution_index__inst_id == -1) {
										institution_data__INSTITUTION_ADDRESS1 = allMatches[1];
										int instCountryIndex = 3;
										if (matches.size() == 3) {
											instCountryIndex = 2;
											Pattern patAddr = Pattern.compile("^(.*?)[A-Z][a-z][a-z\\-]+.*?,\\s([A-Z][a-z][a-z\\-]+)");
											Matcher matchAddr = patAddr.matcher(allMatches[2]);
											Pattern trailSpace = Pattern.compile("\\s+$");
											if (matchAddr.find()) {
												institution_data__INSTITUTION_ZIP = matchAddr.group(1);
												Matcher matchSpace = trailSpace.matcher(institution_data__INSTITUTION_ZIP);
												institution_data__INSTITUTION_ZIP = matchSpace.replaceAll("");
												institution_data__INSTITUTION_CITY = matchAddr.group(2);
											}
										} else {
											Pattern patAddr = Pattern.compile("^(.*?)([A-Z][a-z][A-Za-z\\-]+)");
											Matcher matchAddr = patAddr.matcher(allMatches[2]);
											Pattern trailSpace = Pattern.compile("\\s+$");
											if (matchAddr.find()) {
												institution_data__INSTITUTION_ZIP = matchAddr.group(1);
												Matcher matchSpace = trailSpace.matcher(institution_data__INSTITUTION_ZIP);
												institution_data__INSTITUTION_ZIP = matchSpace.replaceAll("");
												institution_data__INSTITUTION_CITY = matchAddr.group(2);
											}
										}
										institution_data__INSTITUTION_COUNTRY = WordUtils.capitalizeFully(allMatches[instCountryIndex]);
										institution_data__INSTITUTION_COUNTRY = MysqlConnect.GetCountrySQL(dbname, institution_data__INSTITUTION_COUNTRY.trim(), conn);
										if (institution_data__INSTITUTION_COUNTRY.equals("")) {
											/**
											 *  Country does not exist in DB --> comment: "Check country field"
											 */
											comment = "Please check the country name and respective index in the DB - might be a spelling mistake or new country.";
										}
										
										/**
										 * If the country is United States, then also check the state.
										 */
										if (Integer.valueOf(institution_data__INSTITUTION_COUNTRY) == 1) {
											String state = MysqlConnect.GetStateSQL(dbname, conn, allMatches[2]);
											institution_data__INSTITUTION_STATE = state;
											institution_data__INSTITUTION_ZIP = institution_data__INSTITUTION_ZIP.replace(state,"");
											if (institution_data__INSTITUTION_STATE.equals("")) {
												/**
												 * Add to comment field rather than just have it there re-write other comments
												 */
												if (comment.equals("")) {
													comment = "Please check the address information on "+ project__source_url+" to see whether state field is present.";
												} else {
													comment += " Please check the address information on "+ project__source_url +"to see whether state field is present.";
												}
											}

										}
								
									}
									
									/**
									 * Check project data by other fields in case institution and investigator data exist.
									 * If project does not exist then continue through the loop and do not write into the output spreadsheet.
									 */
									String status = MysqlConnect.GetProjectNumberSQL(dbname, project__PROJECT_NUMBER, conn, project__PROJECT_START_DATE, project__PROJECT_END_DATE, investigator_index__inv_id, institution_index__inst_id);
									if (status.equals("Found")) continue;
									
									/**
									* Outputting all data into tab-separated file. 
									* To prevent any mishaps with opening the file, replacing all new lines, tabs and returns in the fields where these can occur.
									* Will be one institution per line.
									*/
									String[] output = {project__PROJECT_NUMBER,project__PROJECT_TITLE,project__source_url,
											project__PROJECT_START_DATE,
											project__PROJECT_OBJECTIVE,
											institution_data__INSTITUTION_NAME,
											institution_data__INSTITUTION_ADDRESS1, institution_data__INSTITUTION_CITY,
											institution_data__INSTITUTION_STATE, institution_data__INSTITUTION_COUNTRY,
											institution_data__INSTITUTION_ZIP,
											String.valueOf(institution_index__inst_id),
											agency_index__aid,comment};
									
										csvout.writeNext(output);
											
								}
								
									
								
							}
							catch (Exception ee) {
								/**
								 * Log exception that page does not exist and link is broken
								 */
								try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logfile, true)))) {
									StringWriter errors = new StringWriter();
									ee.printStackTrace(new PrintWriter(errors));
									out.println(currentDateLog
								    			+"   "
								    			+"Perhaps the link is broken or does not exist - "+infoLink.attr("href")+" ."
								    			+" Here is some help with traceback:"
								    			+errors.toString());
								}catch (IOException e) {

								}
								
							}
							
							
							
							
						} else {
							/**
							 * Signifies that there is no contract award notice yet. Pass for now, it should be re-scraped at a later date.
							 */
						}
						
					}
						
				}
				
			}
		}
		
		csvout.close();
		webClient.close();
		
	}
}
