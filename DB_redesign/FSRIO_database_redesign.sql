
### SOURCE DB:  `fsrioprojects`; ###
### TARGET (redesigned) DB: `fsrioprojects_redesign`; ###

CREATE DATABASE IF NOT EXISTS `fsrioprojects_redesign` /*!40100 DEFAULT CHARACTER SET latin1 */;


### BEGIN publication_data merge and investigator/publication links generation ################################################################################################

# Creating a table with all the publications fields we want to append the data to 
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`publication_data`;
CREATE TABLE `fsrioprojects_redesign`.`publication_data` (
  `pub_id` int(11) NOT NULL AUTO_INCREMENT primary key COMMENT 'The primary identifier for a publication',
  `status` int(1)  NULL DEFAULT '1' COMMENT 'The publication status of the article: 1 - pending, 2 - epub, 3 - printed.',
  `log_number` int(11) NULL COMMENT 'The log number of the publication',
  `title` varchar(512) CHARACTER SET latin1 NOT NULL COMMENT 'The title of the publication',
  `journal` varchar(255) CHARACTER SET latin1 NOT NULL COMMENT 'The name of the journal which accepted the publication',
  `summary` longtext CHARACTER SET latin1 NULL COMMENT 'An interpretive summary of the publication',
  `authors` longtext CHARACTER SET latin1 NOT NULL COMMENT 'The authors of the publication',
  `accepted_year` int(4)  NULL COMMENT 'The year that the article was accepted for publication',
  `accepted_month` int(2)  NULL COMMENT 'The month that the article was accepted for publication',
  `accepted_day` int(2)  NULL COMMENT 'The day that the article was accepted for publication',
  `url` varchar(255) CHARACTER SET latin1 NOT NULL DEFAULT '' COMMENT 'The URL to an online copy of the published publication',
  `naldc_url` varchar(255) NOT NULL DEFAULT '' COMMENT 'The URL of the publication''s full text in the NAL Digital Collections.',
  `publication_year` int(4) NOT NULL COMMENT 'Part of the citation - the journal issue''s year of printing',
  `publication_month` int(2) NOT NULL COMMENT 'Part of the citation - the journal issue''s month of printing',
  `publication_day` int(2) NOT NULL COMMENT 'Part of the citation - the journal issue''s day of printing',
  `volume` varchar(7) NOT NULL COMMENT 'Part of the citation - the journal issue''s volume number',
  `issue` varchar(10) NOT NULL COMMENT 'Part of the citation - the journal issue number',
  `pages` varchar(15) CHARACTER SET latin1 NOT NULL COMMENT 'Part of the citation - the page numbers of the article in the journal issue',
  `created` int(11) NOT NULL COMMENT 'The unix timestamp of when the record was created',
  `changed` int(11) NOT NULL COMMENT 'The UNIX timestamp when the record was last updated.',
  `persistent_pub_id` int(11) NOT NULL COMMENT 'The primary identifier for a publication from ARS and publication_data tables',
  `source` varchar(20) NOT NULL COMMENT 'The original table where publications come from: ARS or publication_data'
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='A list of food safety publications';


# Insert publications data into a combined table within redesign DB
INSERT INTO `fsrioprojects_redesign`.`publication_data` (status, log_number, title, journal, summary, authors, accepted_year, accepted_month, accepted_day, url, naldc_url, publication_year, publication_month,
publication_day, volume, issue, pages, created, changed, persistent_pub_id,source)
SELECT status, log_number, title, journal, summary, authors, accepted_year, accepted_month, accepted_day, url, naldc_url, publication_year, publication_month,
publication_day, volume, issue, pages, created, changed, pub_id,"ars" 
FROM `fsrioprojects`.`ars_publication_data`;

INSERT INTO `fsrioprojects_redesign`.`publication_data` (title, journal, authors, url, naldc_url, publication_year, publication_month,
publication_day, volume, issue, pages, created, changed, persistent_pub_id,source)
SELECT title, journal, authors, url, naldc_url, publication_year, publication_month,
publication_day, volume, issue, pages, created, changed, pub_id,"pubs" 
FROM `fsrioprojects`.`publication_data`;

# Create a crosswalk table with pub_id and pid plus add author info
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`temp_pubs_index`;
CREATE TABLE `fsrioprojects_redesign`.`temp_pubs_index` LIKE `fsrioprojects`.`publication_index`;
ALTER TABLE `fsrioprojects_redesign`.`temp_pubs_index` ADD COLUMN `authors` longtext;

# Insert new pub_ids for publication_data table in crosswalk
INSERT INTO `fsrioprojects_redesign`.`temp_pubs_index` (pid,pub_id,authors)
SELECT pi.pid,tp.pub_id,tp.authors
  FROM `fsrioprojects_redesign`.`publication_data` tp
  LEFT JOIN `fsrioprojects`.`publication_index` pi ON pi.pub_id = tp.persistent_pub_id
WHERE tp.source = "pubs";

# Insert new pub_ids for ars_publication_data table in crosswalk
INSERT INTO `fsrioprojects_redesign`.`temp_pubs_index` (pid,pub_id,authors)
SELECT pi.pid,tp.pub_id,tp.authors
  FROM `fsrioprojects_redesign`.`publication_data` tp
  LEFT JOIN `fsrioprojects`.`ars_publication_index` pi ON pi.pub_id = tp.persistent_pub_id
WHERE tp.source = "ars";

# Create a temp table with investigator info with project ID for further matching with publication authors - take investigator_index and add name info there for ease of matching
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`temp_investigator`;
CREATE TABLE `fsrioprojects_redesign`.`temp_investigator` LIKE `fsrioprojects`.`investigator_index`;
ALTER TABLE `fsrioprojects_redesign`.`temp_investigator` ADD COLUMN `name` varchar(255);

# Insert all investigator names per project
INSERT INTO `fsrioprojects_redesign`.`temp_investigator`
SELECT ii.pid,ii.inv_id,invd.name
  FROM `fsrioprojects`.`investigator_index` ii
  LEFT JOIN `fsrioprojects`.`investigator_data` invd ON invd.ID = ii.inv_id;

# Create a crosswalk table between investigator ID and publication ID without project proxy
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`inv_publication_index`;
CREATE TABLE `fsrioprojects_redesign`.`inv_publication_index` (
   `inv_id` int(11) NOT NULL COMMENT 'The investigator_data.ID of the linked investigator',
   `pub_id` int(11) NOT NULL COMMENT 'The publication_data.pub_id of the linked publication',
   `pid` int(11) NOT NULL COMMENT 'The project.ID of the linked investigator and publication - to be dropped later',
   KEY `inv_id` (`inv_id`),
   KEY `pub_id` (`pub_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Lists investigator/publication relationships';

# Match publication authors to investigator names: we first have to transform the investigator name into format of how it might appear in the authors field
# and then use REGEXP to search the authors field with the investigator name on that project
INSERT into `fsrioprojects_redesign`.`inv_publication_index`
SELECT ti.inv_id,tp.pub_id,ti.pid 
FROM `fsrioprojects_redesign`.`temp_investigator` ti
LEFT JOIN `fsrioprojects_redesign`.`temp_pubs_index` tp ON tp.pid = ti.pid
WHERE tp.authors REGEXP CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(name, ', ', 1), ' ', -1), ' ',
    TRIM( SUBSTR(name, LOCATE(' ', name),2) ));

# Capture all leftovers if they existfrom publication_data that did not map to investigators directly
# (e.g. if the project does not have PI info but only institution and yet has publications associated with it in the DB)
# NOTE: for now, there are no such leftovers but we still include the check process in the code to make sure FSRIO can capture such cases in future if they occur
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`old_publication_index`;
CREATE TABLE `fsrioprojects_redesign`.`old_publication_index` LIKE `fsrioprojects`.`publication_index`;

INSERT INTO `fsrioprojects_redesign`.`old_publication_index`
SELECT pid,pub_id
FROM `fsrioprojects_redesign`.`temp_pubs_index`
WHERE CONCAT(pid,pub_id) NOT IN
    (SELECT CONCAT(tp.pid,tp.pub_id)
    FROM `fsrioprojects_redesign`.`temp_pubs_index` tp 
    LEFT JOIN `fsrioprojects_redesign`.`inv_publication_index` ii ON ii.pid = tp.pid and ii.pub_id = tp.pub_id);

### END publication_data merge and investigator/publication links generation ################################################################################################

### BEGIN investigator and institution data inserts and new crosswalk table generation  #########################################################################################

# Insert data from investigator and institution data in the old DB
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`investigator_data`;
CREATE TABLE `fsrioprojects_redesign`.`investigator_data` LIKE `fsrioprojects`.`investigator_data`;
INSERT INTO `fsrioprojects_redesign`.`investigator_data` SELECT * FROM `fsrioprojects`.`investigator_data`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`investigator_index`;
CREATE TABLE `fsrioprojects_redesign`.`investigator_index` LIKE `fsrioprojects`.`investigator_index`;
INSERT INTO `fsrioprojects_redesign`.`investigator_index` SELECT * FROM `fsrioprojects`.`investigator_index`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`institution_data`;
CREATE TABLE `fsrioprojects_redesign`.`institution_data` LIKE `fsrioprojects`.`institution_data`;
INSERT INTO `fsrioprojects_redesign`.`institution_data` SELECT * FROM `fsrioprojects`.`institution_data`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`institution_index`;
CREATE TABLE `fsrioprojects_redesign`.`institution_index` LIKE `fsrioprojects`.`institution_index`;
INSERT INTO `fsrioprojects_redesign`.`institution_index` SELECT * FROM `fsrioprojects`.`institution_index`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`countries`;
CREATE TABLE `fsrioprojects_redesign`.`countries` LIKE `fsrioprojects`.`countries`;
INSERT INTO `fsrioprojects_redesign`.`countries` SELECT * FROM `fsrioprojects`.`countries`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`states`;
CREATE TABLE `fsrioprojects_redesign`.`states` LIKE `fsrioprojects`.`states`;
INSERT INTO `fsrioprojects_redesign`.`states` SELECT * FROM `fsrioprojects`.`states`;

# Create a separate crosswalk for investigator/institution links. 
# The reason why this is necessary is because we cannot link all institutions to projects via PIs - some data sources only list institutions on their awards/projects.
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`inv_institution_index`;
CREATE TABLE `fsrioprojects_redesign`.`inv_institution_index` (
   `inv_id` int(11) NOT NULL COMMENT 'The investigator_data.ID of the linked investigator',
   `inst_id` int(11) NOT NULL COMMENT 'The institution_data.ID of the linked institution',
   KEY `inv_id` (`inv_id`),
   KEY `inst_id` (`inst_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Lists investigator/institution relationships';

INSERT INTO `fsrioprojects_redesign`.`inv_institution_index`
SELECT DISTINCT ID,INSTITUTION
FROM `fsrioprojects_redesign`.`investigator_data`;

### END investigator and institution data inserts and new crosswalk table generation  #########################################################################################

### BEGIN project data inserts and project_reports data merger  #######################################################################################################

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`project`;
CREATE TABLE `fsrioprojects_redesign`.`project` LIKE `fsrioprojects`.`project`;
INSERT INTO `fsrioprojects_redesign`.`project` SELECT * FROM `fsrioprojects`.`project`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`project_farm_to_table`;
CREATE TABLE `fsrioprojects_redesign`.`project_farm_to_table` LIKE `fsrioprojects`.`project_farm_to_table`;
INSERT INTO `fsrioprojects_redesign`.`project_farm_to_table` SELECT * FROM `fsrioprojects`.`project_farm_to_table`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`farm_to_table_categories`;
CREATE TABLE `fsrioprojects_redesign`.`farm_to_table_categories` LIKE `fsrioprojects`.`farm_to_table_categories`;
INSERT INTO `fsrioprojects_redesign`.`farm_to_table_categories` SELECT * FROM `fsrioprojects`.`farm_to_table_categories`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`status`;
CREATE TABLE `fsrioprojects_redesign`.`status` LIKE `fsrioprojects`.`status`;
INSERT INTO `fsrioprojects_redesign`.`status` SELECT * FROM `fsrioprojects`.`status`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`projecttype`;
CREATE TABLE `fsrioprojects_redesign`.`projecttype` LIKE `fsrioprojects`.`projecttype`;
INSERT INTO `fsrioprojects_redesign`.`projecttype` SELECT * FROM `fsrioprojects`.`projecttype`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`project_category`;
CREATE TABLE `fsrioprojects_redesign`.`project_category` LIKE `fsrioprojects`.`project_category`;
INSERT INTO `fsrioprojects_redesign`.`project_category` SELECT * FROM `fsrioprojects`.`project_category`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`category`;
CREATE TABLE `fsrioprojects_redesign`.`category` LIKE `fsrioprojects`.`category`;
INSERT INTO `fsrioprojects_redesign`.`category` SELECT * FROM `fsrioprojects`.`category`;

###  Merge reports and project_reports tables
DROP TABLE IF EXISTS `fsrioprojects_redesign`.`project_reports`;
CREATE TABLE `fsrioprojects_redesign`.`project_reports` LIKE `fsrioprojects`.`project_reports`;

# Insert all `project_reports` data into the new table
INSERT INTO `fsrioprojects_redesign`.`project_reports`
SELECT *
FROM `fsrioprojects`.`project_reports`;

# Add new column to take into account `report_year` field
ALTER TABLE `fsrioprojects_redesign`.`project_reports` ADD COLUMN `report_year` int(4) DEFAULT NULL;

# Populate the year field for `project_reports` data where it exists in project title
SET sql_safe_updates=0;
UPDATE `fsrioprojects_redesign`.`project_reports`
SET report_year = SUBSTR(title,1,4)
WHERE title REGEXP "^[0-9]";
SET sql_safe_updates=1;

# Insert all `reports` data into project_reports table schema (only fields that match and are needed)
INSERT INTO `fsrioprojects_redesign`.`project_reports` (pid,url,final_report,report_year)
SELECT PROJECT_ID,REPORT_LINK,NULL,REPORT_YEAR
FROM `fsrioprojects`.`reports`;

### END project data inserts and project_reports data merger  #########################################################################################################

### BEGIN agency data inserts #########################################################################################################################################

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`agency_data`;
CREATE TABLE `fsrioprojects_redesign`.`agency_data` LIKE `fsrioprojects`.`agency_data`;
INSERT INTO `fsrioprojects_redesign`.`agency_data` SELECT * FROM `fsrioprojects`.`agency_data`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`agency_index`;
CREATE TABLE `fsrioprojects_redesign`.`agency_index` LIKE `fsrioprojects`.`agency_index`;
INSERT INTO `fsrioprojects_redesign`.`agency_index` SELECT * FROM `fsrioprojects`.`agency_index`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`agency_hierarchy`;
CREATE TABLE `fsrioprojects_redesign`.`agency_hierarchy` LIKE `fsrioprojects`.`agency_hierarchy`;
INSERT INTO `fsrioprojects_redesign`.`agency_hierarchy` SELECT * FROM `fsrioprojects`.`agency_hierarchy`;

DROP TABLE IF EXISTS `fsrioprojects_redesign`.`cache_agency_children`;
CREATE TABLE `fsrioprojects_redesign`.`cache_agency_children` LIKE `fsrioprojects`.`cache_agency_children`;
INSERT INTO `fsrioprojects_redesign`.`cache_agency_children` SELECT * FROM `fsrioprojects`.`cache_agency_children`;

### END agency data inserts ###########################################################################################################################################

### BEGIN clean up ###################################################################################################################################################

# Drop extra columns in some generated tables
ALTER TABLE `fsrioprojects_redesign`.`inv_publication_index` DROP COLUMN pid;
ALTER TABLE `fsrioprojects_redesign`.`investigator_data` DROP COLUMN INSTITUTION;  #since we added a crosswalk between investigator/institution - this field is no longer necessary

# Drop temporary tables with crosswalks between project IDs, investigator info, and publication info
DROP TABLE `fsrioprojects_redesign`.`temp_investigator`;
DROP TABLE `fsrioprojects_redesign`.`temp_pubs_index`;

### END clean up ######################################################################################################################################################