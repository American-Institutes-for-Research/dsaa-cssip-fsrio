# FSRIO
Data automation for USDA FSRIO to parse and scrape data from multiple sources for their food safety Research Projects Database

This is the maven build of the project. To install and run:

1. Clone the directoy to your  local drive.
2. cd into FSRIO/jbt
3. Compile using "mvn compile"

For running upload:
	mvn exec:java -Dexec.mainClass="com.jbt.Run" -Dexec.args="processUpload.cfg password"

For running scraping: 
 	mvn exec:java -Dexec.mainClass="com.jbt.Run" -Dexec.args="process.cfg password"

Thus depending on the cfg file provided, the program automatically runs the upload or scraping script.