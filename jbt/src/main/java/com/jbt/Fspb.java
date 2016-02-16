package com.jbt;

/* Just one thing needs to be fixed - unicode URLs are not well handled by either Jsoup or HtmlUnit:
 * Those links are logged and can be looked up manually to populate the database with this limited number of projects.
 */

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
	public static String main(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
				
		Fspb.scrape(url,outfolder,host,user,passwd,dbname,logfile);
		return "FSPB";
	}
	
	public static void scrape(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"FSPB_"+currentStamp+".csv"),'\t');
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
		
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		
		Elements projLinks = doc.select(".projects");
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
				
			//Declare needed strings
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
			
			//Institution variables
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_ADDRESS1 = "";
			String institution_data__INSTITUTION_CITY = "";
			String institution_data__INSTITUTION_COUNTRY = "";
			String institution_data__INSTITUTION_ZIP = "";
			String institution_data__INSTITUTION_STATE = "";
			
			//PI variables
			String investigator_data__name = "";
			
			//Processing variables
			String piInfo = "";
			String piLastName = "";
			String piFirstName = "";
			String piName = "";
			String query = "";
			
			//Project source URL
			project__source_url = "http://www.safefood.eu"+projLink.attr("href");
			
			//Project number
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
			
			
			//Dates entered and updated
			DateFormat dateFormatEntered = new SimpleDateFormat("yyyy-MM-dd");
			String currentEntered = dateFormatEntered.format(current);
			project__DATE_ENTERED = currentEntered;
			project__LAST_UPDATE = currentDateLog;
			
			//Project title
			try {
				project__PROJECT_TITLE = finaldoc.select("h3").text();
			}
			catch (Exception exx) {
				project__PROJECT_TITLE = "";
			}
			//Project objective, investigator and institution
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
			
			//Investigator name
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
			
			
			//Institution name plus city
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
			
			//Project dates
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
			
			//Convert duration into day counts
			int projDays = 0;
			if (projDura.split(" ")[1].startsWith("week")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*7;
			} else if (projDura.split(" ")[1].startsWith("month")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*30;
			} else if (projDura.split(" ")[1].startsWith("year")) {
				projDays = Integer.valueOf(projDura.split(" ")[0])*365;
			} else {
				//Log an error with dates - projDura
			}
			
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
			
			//Check institution in MySQL DB
			query = "SELECT * from "+dbname+".institution_data where institution_name like \""+institution_data__INSTITUTION_NAME+"\"";
			Connection conn = MysqlConnect.connection(host,user,passwd);
			ResultSet result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
			try {
				result.next();
				institution_index__inst_id = result.getInt(1);
			}
			catch (Exception e) {

			}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}
			
			//Check PI name in MySQL DB
			query = "SELECT * FROM "+dbname+".investigator_data where name like \""+investigator_data__name+"\"";
			conn = MysqlConnect.connection(host,user,passwd);
			result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
			try {
				result.next();
				investigator_index__inv_id = result.getInt(1);
				if (institution_index__inst_id == -1) {
					String instindex = result.getString(5);
					ResultSet checkInst = MysqlConnect.sqlQuery("SELECT * from "+dbname+".institution_data where id = \""+instindex+"\"",conn,host,user,passwd);
					checkInst.next();
					String existInst = checkInst.getString(2);
					Pattern patInst = Pattern.compile(existInst);
					Matcher matchInst = patInst.matcher(piInfo);
					if (matchInst.find()) {
						institution_index__inst_id = Integer.parseInt(instindex);
					}
				}
			}
			catch (Exception e) {
				try {
					query = "SELECT * FROM "+dbname+".investigator_data where name regexp \"^"+piLastName+", "+piFirstName.substring(0,1)+"\"";
					result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
					result.next();
					investigator_index__inv_id = result.getInt(1);
					if (institution_index__inst_id == -1) {
						String instindex = result.getString(5);
						ResultSet checkInst = MysqlConnect.sqlQuery("SELECT * from "+dbname+".institution_data where id = \""+instindex+"\"",conn,host,user,passwd);
						checkInst.next();
						String existInst = checkInst.getString(2);
						Pattern patInst = Pattern.compile(existInst);
						Matcher matchInst = patInst.matcher(piInfo);
						if (matchInst.find()) {
							institution_index__inst_id = Integer.parseInt(instindex);
						}
					}
				}
				catch (Exception except) {
					
				}	
			}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}
			
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
				//Check if project exists in DB
				query = "SELECT p.PROJECT_NUMBER FROM "+dbname+".project p left outer join "+dbname+".investigator_index ii on ii.pid = p.id where p.PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\""
						+ " and p.PROJECT_START_DATE = \""+project__PROJECT_START_DATE+"\" and p.PROJECT_END_DATE = \""+project__PROJECT_END_DATE+"\" and ii.inv_id = \""+String.valueOf(investigator_index__inv_id)+"\"";
				conn = MysqlConnect.connection(host,user,passwd);
				result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
				try {
					result.next();
					String number = result.getString(1);
					continue;
				}
				catch (Exception ex) {;}
				finally {
					if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
				}
			}
			
			//Write resultant values into CSV
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

