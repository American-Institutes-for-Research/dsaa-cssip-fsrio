package com.jbt;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
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

public class Omafra {

	public static String main(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		
		String pat = "english/research/foodsafety/\\d+";
		Omafra.scrape(url,pat,outfolder,host,user,passwd,dbname);
		return "OMAFRA";
	}
	
	public static void scrape(String url, String pat, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"Omafra_"+currentStamp+".csv"),'\t');
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE","project__PROJECT_END_DATE",
				"project__PROJECT_MORE_INFO","project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"investigator_data__name",
				"institution_index__inst_id","investigator_index__inv_id",
				"agency_index__aid","comment"};
		csvout.writeNext(header);
		
		//Initiate webClient
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
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
						
						HtmlPage finalPage = webClient.getPage("http://www.omafra.gov.on.ca/"+linkInside.attr("href"));
						Document finaldoc = Jsoup.parse(finalPage.asXml());

						Element content = finaldoc.getElementById("right_column");
						
						//Declare needed strings
						String project__PROJECT_NUMBER = "";
						String project__PROJECT_TITLE = "";
						String project__source_url = "";
						String project__PROJECT_START_DATE = "";
						String project__PROJECT_END_DATE = "";
						String project__PROJECT_MORE_INFO = "";
						String project__PROJECT_OBJECTIVE = "";
						String project__PROJECT_ABSTRACT = "";
						String project__LAST_UPDATE = "";
						String project__DATE_ENTERED = "";
						String institution_data__INSTITUTION_NAME = "";
						String agency_index__aid = "";
						int institution_index__inst_id = -1;
						int investigator_index__inv_id = -1;
						String comment = "";
						String investigator_data__name = "";
						
						//Processing variables
						String instpidata = "";
						String piName = "";
						String instInfo = "";
						String query = "";
						String piLastName = "";
						String piFirstName = "";
						
						//Title
						Elements titleElem = content.getElementsByTag("h2");
						String title = titleElem.text();
						project__PROJECT_NUMBER = title.split(" - ")[0];
						project__PROJECT_TITLE = title.split(" - ")[1];
						
						//Agency index
						agency_index__aid = "69";
						
						//Source_url
						project__source_url = "http://www.omafra.gov.on.ca/"+linkInside.attr("href");
						
						//Date stamp
						project__LAST_UPDATE = dateFormat.format(current);
						DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
						project__DATE_ENTERED = dateFormatEnter.format(current);
						
						
						Element divElem = content.child(0);
						Elements allElem = divElem.children();
						for (Element elem : allElem) {
							//Start and end date
							Pattern patdate = Pattern.compile("Start-End Date:\\s+(\\d+)-(\\d+)");
							Matcher matcherDate = patdate.matcher(elem.text());
							if (matcherDate.find()) {
								project__PROJECT_START_DATE = matcherDate.group(1);
								project__PROJECT_END_DATE = matcherDate.group(2);
							}
							
							//Expected benefits (more info)
							Pattern patBenefits = Pattern.compile("Expected Benefits");
							Matcher matcherBenefits = patBenefits.matcher(elem.text());
							if (matcherBenefits.find()) {
								project__PROJECT_MORE_INFO = elem.nextElementSibling().text();
							}
							
							//Project objectives
							Pattern patObjectives = Pattern.compile("Objectives");
							Matcher matcherObjectives = patObjectives.matcher(elem.text());
							if (matcherObjectives.find()) {
								project__PROJECT_OBJECTIVE = elem.nextElementSibling().text();
							}
							
							//Abstract / description
							Pattern patAbstract = Pattern.compile("Description");
							Matcher matcherAbstract = patAbstract.matcher(elem.text());
							if (matcherAbstract.find()) {
								project__PROJECT_ABSTRACT = elem.nextElementSibling().text();
							}
							
							//Institution and PI
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

						//Check institution in MySQL DB
						query = "SELECT * from "+dbname+".institution_data where institution_name like \""+instInfo+"\"";
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
						query = "SELECT * FROM "+dbname+".investigator_data where name like \""+piName+"\"";
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
								Matcher matchInst = patInst.matcher(instpidata);
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
									Matcher matchInst = patInst.matcher(instpidata);
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
							institution_data__INSTITUTION_NAME = instInfo;
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
						} else {
							investigator_data__name = piName;
							
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
