package com.jbt;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.util.Arrays;

public class Run {
	
	public static void main(String[] args) throws IOException,SAXException,ParserConfigurationException,Exception {

		Properties prop = new Properties();
		InputStream in = new FileInputStream(new File(args[0])); 
		String option = args[2];
		prop.load(in);
		in.close();
		String outfolder = prop.getProperty("OUTPUT_FOLDER");
		//Get MySQL credentials
		String host = prop.getProperty("MYSQL_HOST");
		String user = prop.getProperty("MYSQL_USERNAME");
		String passwd =  args[1];
		String dbname = prop.getProperty("MYSQL_DBNAME");
		String logfile = prop.getProperty("LOG_FILE");
		
		if(option.equalsIgnoreCase("scrape")) {
		String[] dataSources = {"CampdenBri","Defra","Efsa","Esrc","Fsa","Fspb","NIH","NSF","Omafra","Relu","AHDB"};
		String[] sources = prop.getProperty("SOURCES").split(",");
		if (sources[0].equals("all")) {
			sources = dataSources;
		}
		
		int len = sources.length;
		for (String source : sources) {
			if (Arrays.asList(dataSources).contains(source)) {
				if (source.equals("CampdenBri")) {
					System.out.println("CampdenBRI website scraper is running...");
					try {
						CampdenBri.campdenMain(outfolder,prop.getProperty("CAMPDENBRI_MAIN_LINKS").split(","),
								prop.getProperty("CAMPDENBRI_ADD_LINKS").split(","),host,user,passwd,dbname);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in CampdenBri. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("CampdenBRI website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Defra")) {
					System.out.println("DEFRA website scraper is running...");
					try {
						Defra.defraMain(prop.getProperty("DEFRA_MAINPAGE_URL"),outfolder,host,user,passwd,dbname);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in DEFRA. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("DEFRA website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Efsa")) {
					System.out.println("EFSA website scraper is running...");
					try {
						Efsa.efsaMain(prop.getProperty("EFSA_MAINPAGE_URL"),outfolder,host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in EFSA. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("EFSA website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Esrc")) {
					System.out.println("ESRC website scraper is running...");
					try {
						Esrc.esrcMain(prop.getProperty("ESRC_MAINPAGE_URL"),outfolder,host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in ESRC. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("ESRC website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Fsa")) {
					System.out.println("FSA website scraper is running...");
					try {
						Fsa.fsaMain(prop.getProperty("FSA_LINKS").split(","),outfolder,host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in FSA. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("FSA website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Fspb")) {
					System.out.println("FSPB website scraper is running...");
					try {
						Fspb.fspbMain(prop.getProperty("FSPB_MAINPAGE_URL"),outfolder,host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in FSPB. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("FSPB website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("NIH")) {
					System.out.println("NIH parser is running...");
					try {
						NIH.nihMain(prop.getProperty("INPUT_FOLDER_NIH"),prop.getProperty("INPUT_FOLDER_NIH_ABSTRACTS"),outfolder,host,user,passwd,dbname);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in NIH. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("NIH award files parsed successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("NSF")) {
					System.out.println("NSF parser is running...");
					try {
						NSF.nsfMain(prop.getProperty("INPUT_FOLDER_NSF"),outfolder,host,user,passwd,dbname);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in NSF. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("NSF award files parsed successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Omafra")) {
					System.out.println("OMAFRA website scraper is running...");
					try {
						Omafra.omafraMain(prop.getProperty("OMAFRA_MAINPAGE_URL"),outfolder,host,user,passwd,dbname);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in OMAFRA. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("OMAFRA website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("Relu")) {
					System.out.println("RELU website scraper is running...");
					try {
						Relu.reluMain(prop.getProperty("RELU_LINKS").split(","),outfolder,host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in RELU. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("RELU website scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
				if (source.equals("AHDB")) {
					System.out.println("AHDB website scraper is running...");
					try {
						AHDB.ahdbMain(outfolder,prop.getProperty("AHDB_LINKS").split(","),host,user,passwd,dbname,logfile);
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("An exception occurred in AHDB. This probably was nothing critical,"
								+ " try rerunning the program again. Please contact your IT support for further information");
					}
					len = len-1;
					System.out.println("AHDB websites scraped successfully... "+len+" source(s) left to scrape/parse.");
				}
			} else {
				System.out.println("The source \""+source+"\" is invalid. Please check that"
						+ " the source and class names are valid as described in config_file");
			}
			
		}

	}
	else if(option.equalsIgnoreCase("upload"))	{
		File folder = new File(outfolder);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
      		if (listOfFiles[i].isFile())
        		upload.main(listOfFiles[i], host, user, passwd, dbname, logfile);
    }
		
	}
	else {
		System.out.println("The third argument must be one of upload or scrape");
	}	

		
	}

}
