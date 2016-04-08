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

public class Efsa {

	public static String efsaMain(String url, String outfolder, String host, String user, String passwd, String dbname, String logfile) throws IOException {
		
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		Connection conn = MysqlConnect.connection(host,user,passwd);
		String pat = "en/tender.*?/tender/|en/node/915681";
		Efsa.scrape(url,pat,outfolder,conn,dbname,logfile);
		if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}		
		return "EFSA";
	}
	
	public static void scrape(String url, String pat, String outfolder, Connection conn, String dbname, String logfile) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);
		
		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"EFSA_"+currentStamp+".csv"),'\t');
		String[] header = {"project__PROJECT_NUMBER","project__PROJECT_TITLE",
				"project__source_url",
				"project__PROJECT_START_DATE",
				"project__PROJECT_OBJECTIVE",
				"institution_data__INSTITUTION_NAME",
				"institution_data__INSTITUTION_ADDRESS1","institution_data__INSTITUTION_CITY",
				"institution_data__INSTITUTION_STATE","institution_data__INSTITUTION_COUNTRY",
				"institution_data__INSTITUTION_ZIP",
				"institution_index__inst_id",
				"agency_index__aid"};
		csvout.writeNext(header);
		
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
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
				//Check whether all links are being captured given pattern - should be 20 per page except for very last
				int checkNums = 0;
				
				for (Element link : links) {
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
								//Log that there is simply no link there
							}
						}
						if (infoLink != null) {
							try {
								//Get to next phase
								HtmlPage finalPage = webClient.getPage(infoLink.attr("href"));
								Document finaldoc = Jsoup.parse(finalPage.asXml());
								
								Element content = finaldoc.getElementById("fullDocument");
								
								//Declare needed strings
								String project__PROJECT_NUMBER = "";
								String project__PROJECT_TITLE = "";
								String project__source_url = "";
								String project__PROJECT_START_DATE = "";
								String project__PROJECT_END_DATE = "";
								String project__PROJECT_OBJECTIVE = "";
								String project__LAST_UPDATE = "";
								String project__DATE_ENTERED = "";
								String project__PROJECT_FUNDING = "";
								String agency_index__aid = "122";
								int institution_index__inst_id = -1;
								String comment = "";
								
								//Institution variables
								String institution_data__INSTITUTION_NAME = "";
								String institution_data__INSTITUTION_ADDRESS1 = "";
								String institution_data__INSTITUTION_CITY = "";
								String institution_data__INSTITUTION_COUNTRY = "";
								String institution_data__INSTITUTION_ZIP = "";
								String institution_data__INSTITUTION_STATE = "";
								
								//Processing variables
								String instInfo = "";
								String query = "";
								
								//Project source URL
								project__source_url = infoLink.attr("href");
								
								//Project number
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
								
								//Project start date
								Element dateElem = content.select("span:containsOwn(Date of contract award)").first().nextElementSibling();
								String startDate = dateElem.text();
								Pattern patDate = Pattern.compile("(\\d+)$");
								Matcher matchDate = patDate.matcher(startDate);
								while (matchDate.find()) {
									project__PROJECT_START_DATE = matchDate.group(1);
								}
								
								//Check if project exists in DB
								query = "SELECT PROJECT_NUMBER FROM  "+dbname+"project where PROJECT_NUMBER = ? and PROJECT_START_DATE = ?;";
								ResultSet result =null;
								try {
									PreparedStatement preparedStmt = conn.prepareStatement(query);
									preparedStmt.setString(1, project__PROJECT_NUMBER);
									preparedStmt.setString(2, project__PROJECT_START_DATE);
									result = preparedStmt.executeQuery();
									result.next();
									String number = result.getString(1);
									continue;
								}
								catch (Exception ex) {;}

								
								//Title
								Element titleElem = content.select("span:containsOwn(Title attributed to)").first();
								project__PROJECT_TITLE = titleElem.nextElementSibling().text();
								project__PROJECT_TITLE = project__PROJECT_TITLE.replace(project__PROJECT_NUMBER,"");
								matchSymb = badSymb.matcher(project__PROJECT_TITLE);
								project__PROJECT_TITLE = matchSymb.replaceAll("");
								
								//Project abstract
								Element abstElem = content.select("span:containsOwn(Short description of)").first();
								project__PROJECT_OBJECTIVE = abstElem.nextElementSibling().text();
								matchSymb = badSymb.matcher(project__PROJECT_OBJECTIVE);
								project__PROJECT_OBJECTIVE = matchSymb.replaceAll("");
								
								//Date stamp
								project__LAST_UPDATE = dateFormat.format(current);
								DateFormat dateFormatEnter = new SimpleDateFormat("yyyy-MM-dd");
								project__DATE_ENTERED = dateFormatEnter.format(current);
								
								//Institution info - can be several if multiple contracts awarded under one tender
								Elements instElems = content.select("span:containsOwn(Name and address of economic operator)");
								for (Element instElem : instElems) {
									institution_data__INSTITUTION_NAME = "";
									institution_data__INSTITUTION_ADDRESS1 = "";
									institution_data__INSTITUTION_CITY = "";
									institution_data__INSTITUTION_COUNTRY = "";
									institution_data__INSTITUTION_ZIP = "";
									institution_data__INSTITUTION_STATE = "";
									institution_index__inst_id = -1;
									
									Element instContainer = instElem.nextElementSibling();
									instInfo = StringEscapeUtils.unescapeHtml4(instContainer.children().select("p").html().toString());
									List<String> matches = new ArrayList<String>();
									for (String instInfoElem : instInfo.split("<br>")) {
										matches.add(instInfoElem);
									}
									
									String[] allMatches = new String[matches.size()];
									allMatches = matches.toArray(allMatches);
									institution_data__INSTITUTION_NAME = allMatches[0];
									
									//Check institution in MySQL DB
									query = "SELECT * from  "+dbname+"institution_data where institution_name like ?;";
									result = null;
									try {
										PreparedStatement preparedStmt = conn.prepareStatement(query);
										preparedStmt.setString(1, institution_data__INSTITUTION_NAME);
										result = preparedStmt.executeQuery();
										result.next();
										institution_index__inst_id = result.getInt(1);
									}
									catch (Exception e) {
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
										query = "SELECT * FROM  "+dbname+"countries WHERE COUNTRY_NAME = ?";
										result = null;
										try {
											PreparedStatement preparedStmt = conn.prepareStatement(query);
											preparedStmt.setString(1, institution_data__INSTITUTION_COUNTRY.trim());
											result = preparedStmt.executeQuery();
											result.next();
											institution_data__INSTITUTION_COUNTRY = result.getString(1);
										}
										catch (Exception exc) {
											// Country does not exist in DB --> comment: "Check country field"
											comment = "Please check the country name and respective index in the DB - might be a spelling mistake or new country.";
										}
										if (Integer.valueOf(institution_data__INSTITUTION_COUNTRY) == 1) {
											try {
												query = "SELECT abbrv FROM  "+dbname+"states";
												PreparedStatement preparedStmt = conn.prepareStatement(query);
												result = preparedStmt.executeQuery();
												while (result.next()) {
													String state = result.getString(1);
													Pattern patState = Pattern.compile("("+state+")");
													Matcher matchState = patState.matcher(allMatches[2]);
													if (matchState.find()) {
														institution_data__INSTITUTION_STATE = state;
														institution_data__INSTITUTION_ZIP = institution_data__INSTITUTION_ZIP.replace(state,"");
														break;
													}
													
												}
												if (institution_data__INSTITUTION_STATE == "") {
													//Add to comment field rather than just have it there re-write other comments
													comment = "Please check the address information on project_source_url to see whether state field is present.";
												}
											}
											catch (Exception exc) {
												
											}
											
										}
								
									}

									//Write resultant values into CSV
									String[] output = {project__PROJECT_NUMBER,project__PROJECT_TITLE,project__source_url,
											project__PROJECT_START_DATE,
											project__PROJECT_OBJECTIVE,
											institution_data__INSTITUTION_NAME,
											institution_data__INSTITUTION_ADDRESS1, institution_data__INSTITUTION_CITY,
											institution_data__INSTITUTION_STATE, institution_data__INSTITUTION_COUNTRY,
											institution_data__INSTITUTION_ZIP,
											String.valueOf(institution_index__inst_id),
											agency_index__aid};
									
										csvout.writeNext(output);
											
								}
								
									
								
							}
							catch (Exception ee) {
								//Log exception that page does not exist and link is broken
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
							//Handle if there is no contract award notice yet; perhaps just pass
						}
						
					}
						
				}
				if (i!=numPages && checkNums != 20) {
					//Log error that some links were missed and they have to double check somehow
				}
				
			}
		}
		
		csvout.close();
		webClient.close();
		
	}
}
