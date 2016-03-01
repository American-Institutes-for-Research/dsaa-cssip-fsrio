# FSRIO
Data automation for USDA FSRIO to parse and scrape data from multiple sources for their food safety Research Projects Database

Dependencies:  
1. Maven 3.0 or higher  
2. JDK 1.8  

Before running, please ensure that the configuration files are setup properly, specifically, that the MYSQL host, database and username have been updated.  

There are default values for all other options, but ensure that the values are what you want them to be.  

## Building and Running from Scratch

This is the maven build of the project. To install and run:

1. Clone the directoy to your  local drive.
2. cd into FSRIO/jbt
3. Compile using "mvn compile"



For running upload:
	mvn exec:java -Dexec.mainClass="com.jbt.Run" -Dexec.args="processUpload.cfg password"

For running scraping: 
 	mvn exec:java -Dexec.mainClass="com.jbt.Run" -Dexec.args="process.cfg password"

Thus depending on the cfg file provided, the program automatically runs the upload or scraping script.


## Using prebuilt jar file

To avoid compiling, the executable jar can be used to run the program as follows:  
1. Clone the directoy to your  local drive.  
2. cd into FSRIO/jbt  


For running upload:
	java -jar jbt-0.0.1-SNAPSHOT-jar-with-dependencies.jar "processUpload.cfg" "password"

For running scraping: 
 	java -jar jbt-0.0.1-SNAPSHOT-jar-with-dependencies.jar "process.cfg" "password"

How to open the tsv files generated in Excel:  
1. Open Excel, go to Data tab.  
2. Click From Text.  
3. Choose the appropriate file.  
4. Click next, then select tab as the delimiter.
5. Click Finish.
