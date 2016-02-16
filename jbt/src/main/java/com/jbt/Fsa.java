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

	public static String main(String[] links, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		
		Fsa.scrape(links,outfolder,host,user,passwd,dbname,logfile);
		return "FSA";
	}

	public static void scrape(String[] links,String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"FSA_"+currentStamp+".csv"),'\t');
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_index__inst_id",
				"agency_index__aid","comment"};
		csvout.writeNext(header);
		
		//Initiate webClient
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
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
            				
	            			
	            			//Declare needed strings
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
	    					int institution_index__inst_id = -1;
	    					String comment = "";
	    					
	    					//Institution variables
							String institution_data__INSTITUTION_NAME = "";
							
	    					//Processing variables
							String query = "";
	    					
	    					//Project URL
	    					project__source_url = "http://www.food.gov.uk"+projLink.attr("href");
	    					
	    					//Project number
	    					project__PROJECT_NUMBER = finaldoc.select("strong:containsOwn(Project code)").first().parent().text().replace("Project code: ","");
	    					String projInfo = "";
	    					if (project__PROJECT_NUMBER.contains("Study Duration")) {
	    						projInfo = finaldoc.select("strong:containsOwn(Project code)").first().parent().text();
	    						Pattern patNum = Pattern.compile("Project code\\:\\s+(.*?)");
	    						Matcher matchNum = patNum.matcher(projInfo);
	    						project__PROJECT_NUMBER = matchNum.group(1);
	    					}
	    					
	    					//Project start and end dates
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
	        				
	        				//Check if project exists in DB
	    					query = "SELECT PROJECT_NUMBER FROM "+dbname+".project where PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\""
	    							+ " and PROJECT_START_DATE = \""+project__PROJECT_START_DATE+"\" and PROJECT_END_DATE = \""+project__PROJECT_END_DATE+"\"";
	    					Connection conn = MysqlConnect.connection(host,user,passwd);
	    					ResultSet result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
	    					try {
	    						result.next();
	    						String number = result.getString(1);
	    						continue;
	    					}
	    					catch (Exception ex) {;}
	    					finally {
	    						if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
	    					}
	    					
    						//Dates entered and updated
        					DateFormat dateFormatEntered = new SimpleDateFormat("yyyy-MM-dd");
        					String currentEntered = dateFormatEntered.format(current);
        					project__DATE_ENTERED = currentEntered;
        					project__LAST_UPDATE = dateFormat.format(current);
        					
        					//Project title
        					project__PROJECT_TITLE = finaldoc.select("#page-title").text();

        					
	        					
	        				try {
        						//Institution name
	        					if (projInfo.equals("")) {
	        						institution_data__INSTITUTION_NAME = finaldoc.select("strong:containsOwn(Contractor)").first().parent().text().replace("Contractor: ","");
	        					} else {
	        						Pattern patInst = Pattern.compile("Contractor\\:\\s+(.*?)");
		        					Matcher matchInst = patInst.matcher(projInfo);
		        					while (matchInst.find()) {
		        						institution_data__INSTITUTION_NAME = matchInst.group(1);
		        					}
	        					}
	        					//Check institution in MySQL DB
								query = "SELECT * from "+dbname+".institution_data where institution_name like \""+institution_data__INSTITUTION_NAME+"\"";
								conn = MysqlConnect.connection(host,user,passwd);
								result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
								try {
									result.next();
									institution_index__inst_id = result.getInt(1);
								}
								catch (Exception e) {
									comment = "Institution not in the DB; please collect information manually and populate in institution_data table.";
								}
								finally {
									if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
								}
	        				} catch (Exception ee) {
	        					comment = "No institution information available; please check "+ project__source_url + " to identify if any additional information can be retrieved.";
	        				}
							
							//Project objective
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
							
							//Project more info
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
							
							//Write resultant values into CSV
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
