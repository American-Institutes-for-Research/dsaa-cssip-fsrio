package com.jbt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
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
	public static String main(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		Logger logger = Logger.getLogger ("");
		logger.setLevel (Level.OFF);
		
		Esrc.scrape(url,outfolder,host,user,passwd,dbname);
		return "ESRC";

	}
	
	public static void scrape(String url, String outfolder, String host, String user, String passwd, String dbname) throws IOException {
		//Get current date to assign filename
		Date current = new Date();
		DateFormat dateFormatCurrent = new SimpleDateFormat("yyyyMMdd");
		String currentStamp = dateFormatCurrent.format(current);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDateLog = dateFormat.format(current);

		CSVWriter csvout = new CSVWriter(new FileWriter(outfolder+"ESRC_"+currentStamp+".csv"),'\t');
		
		String[] header = {"project__PROJECT_NUMBER", "project__PROJECT_TITLE", "project__source_url",
				"project__PROJECT_START_DATE", "project__PROJECT_END_DATE", "project__PROJECT_OBJECTIVE",
				"agency_index__aid",
				"investigator_data__name", "investigator_index__inv_id" };
		csvout.writeNext(header);

		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setTimeout(50000);
		
		HtmlPage startPage = webClient.getPage(url);
		Document doc = Jsoup.parse(startPage.asXml());
		Elements links = doc.select("a");

		for (Element link : links) {
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

			String comment = "";

			//Institution variables
			String institution_data__INSTITUTION_NAME = "";
			String institution_data__INSTITUTION_COUNTRY = "184";

			//Processing variables
			String query = null;
			String piName = null;
			String piLastName = null;
			String piFirstName = null;
			
			//Project source URL
			if (!link.attr("href").startsWith("/grants")) {
				continue;
			}
			project__source_url = "http://researchcatalogue.esrc.ac.uk/"+link.attr("href");

			
			HtmlPage nextPage = webClient.getPage(project__source_url);
			Document finaldoc = Jsoup.parse(nextPage.asXml());
			
			project__PROJECT_TITLE = finaldoc.select("div[class=page-header]").text();
			project__PROJECT_NUMBER = link.attr("href").replace("/grants/", "").replace("/read", "");
			
			project__PROJECT_START_DATE = finaldoc.select("dt:contains(Start date) + dd").first().text();
			project__PROJECT_END_DATE = finaldoc.select("dt:contains(End date) + dd").first().text();
			
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
			
			//Check in DB whether PI exists
			String GetInvestigatorSQL = "SELECT ID FROM " + dbname + ".investigator_data WHERE NAME LIKE \"" +  investigator_data__name + "\";";
			Connection conn = MysqlConnect.connection(host,user,passwd);
			ResultSet rs6 = MysqlConnect.sqlQuery(GetInvestigatorSQL,conn,host,user,passwd);
			try {
				rs6.next();
				investigator_index__inv_id = Integer.parseInt(rs6.getString(1));
			}
			catch (Exception e) {
				query = "SELECT * FROM "+dbname+".investigator_data where name regexp \"^"+piLastName+", "+piFirstName.substring(0,1)+"\"";
				ResultSet result = MysqlConnect.sqlQuery(query,conn,host,user,passwd);
				try {
					result.next();
					investigator_index__inv_id = result.getInt(1);
				}
				catch (Exception except) {;}	
			}
			finally {
				if (conn != null) try { conn.close(); } catch (SQLException logOrIgnore) {}
			}
			
			//Project number check within DB	
			query = "SELECT PROJECT_NUMBER FROM "+dbname+".project where PROJECT_NUMBER = \""+project__PROJECT_NUMBER+"\"";
			conn = MysqlConnect.connection(host,user,passwd);
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
			
			if (investigator_index__inv_id == -1) {
				
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
