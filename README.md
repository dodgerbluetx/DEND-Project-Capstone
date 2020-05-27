# Data Engineering Nanodegree
## Project 06 - Capstone


## Overview

The purpose of the data engineering capstone project is to give you a chance to
combine what you've learned throughout the program. This project will be an
important part of your portfolio that will help you achieve your data
engineering-related career goals.

For this project, we are given the choice to use provided datasets or to use
our own datasets.  I have chosen to use the provided datasets but to enhance
them with data from the public domain for additional data points.


## Project Scope and Data Gathering

* The main data source for this project will be the i94 immigration data set
    provided in SAS (statistical analytics software) format.  There is one
    years (2016) worth of data partitioned by months.

* The supplementary data file I94_SAS_Labels_Descriptions.SAS contains field
    descriptions, and additionally contains valid values for select fields.
    This data will be used in some dimensions tables.

* There are no provided definitions for the various visa types, so an external
source will be used to provide context.

* Demographics data sourced from us-cities-demographics.csv will be imported
    to provide demographic data points.

* Finally, airline IATA code data will be used sourced from Wikipedia to provide
    a correlation between the various immigration airport/airline data.

* The final data sets will be useful for analytics purposes, possibly providing
insights into immigration trends.


## Data Exploration and Assessment

The following describes each data source/file and the steps taken to
prepare/clean the data.

* The main SAS i94 immigration data set is imported and cleaned. Columns
deemed not usable or containing data that is not valid are removed, including;
cicid, count, dtadfile, entdepu, matflag, insnum, dtaddto, and admnum. Drop
rows with null values for i94mode and i94addr.

* Use I94_SAS_Labels_Descriptions.SAS to manually copy the i94 port data out
and into a separate CSV file, i94port.csv. Used regular expressions and atom
text editor to convert the provided data into a three-column CSV, containing
port_id, port_name, and port_state. Many special characters, spaces, extra
commas, etc, had to be removed in order to obtain clean list.

* Use I94_SAS_Labels_Descriptions.SAS to manually copy the i94cit/i94res data
out and into a separate CSV file, i94cnty.csv. Used regular expressions and
atom text editor to convert the provided data into a two-column CSV, containing
country_id and country_name. Many special characters, spaces, extra
commas, etc, had to be removed in order to obtain clean list.

* Use I94_SAS_Labels_Descriptions.SAS to manually copy the i94mode data out
and into a separate CSV file, i94mode.csv. Used regular expressions and atom
text editor to convert the provided data into a two-column CSV, containing
mode_id and mode_name. This list was fairly clean to being with and did not
require much cleaning effort.

* Use I94_SAS_Labels_Descriptions.SAS to manually copy the i94addr data
out and into a separate CSV file, i94state.csv. Used regular expressions and
atom text editor to convert the provided data into a two-column CSV, containing
state_id and state_name.

* Use I94_SAS_Labels_Descriptions.SAS to manually copy the i94visa data
out and into a separate CSV file, i94visacat.csv. Used regular expressions and
atom text editor to convert the provided data into a two-column CSV, containing
visa_id and visa_category.

* Used the Visa Categories list located [here](https://visaguide.world/us-visa/)
to build a CSV file, i94visatype.csv.  There are two columns in this CSV,
visa_type_id, and visa_type.

* Using the provided us-cities-demographics.csv CSV file, cleaned up to replace
the ';' delimiter with a comma delimiter, and replaced the column headers.
There are twelve columns in this CSV; city, state, median_age, male_population,
female_population, total_population, number_of_veterans, foreign_born,
average_household_size, state_code, race, count.

* Airline IATA code data was sourced from [Wikipedia](https://en.wikipedia.org/wiki/List_of_airline_codes).  The data was cleaned to remove all special characters
and to align the data columns.  Additionally, all rows that did not contain
a valid IATA code were removed.


## Data Model Definition

I chose to use Spark to process the SAS data into Parquet.  The CSV data sets
are read and copied using the psycopg2 PostgreSQL library, and all of the final
data are transferred into an AWS Redshift cluster.  All data are stored in S3
as the initial data source.  I chose this model as I believe it is streamlined
and suitable to handle large amounts of data growth.

* High-level pipeline steps:
    * EMR cluster Spark instance ingests i94 SAS data from S3 and writes to
    Parquet in S3.
    * Import i94 Parquet data and stage into Redshift table.
    * Import CSV data files from S3, and load into Redshift dims tables.
    * Write final i94 data from staging source to Redshift facts table.
    * Perform data quality checks.

## ETL Details

In order to run the scripts included in this project, both and EMR and
Redshift clusters are required and an AWS account.

Included scripts:

  `manage_aws` - Used to automate the building and deletion of a
    Redshift cluster.

  `analyze_data.py` - Used to do some cursory analysis of the raw i94 data,
    not required for use for the ETL process.

  `create_tables.py` - Used to build the required SQL table structure in the
    Redshift cluster.

  `etl.py` - Main ETL process, retrieves, transforms and loads the data sets.

  `sql_queries` - Contains the queries used to create, copy, load and insert
    data as part of the overall ETL.

  `./data` - Data sets used by this process.


## Environment Preparation

Below are the steps required to prepare the EMR/Redshift cluster and to run the
jobs.


1. Start by creating an EMR cluster with following specifications:
    * m5.xlarge (1 master, 3 worker)
    * Spark 2.4.5
    * Hadoop 2.8.5 Yarn
    * Zeppelin 0.8.2


2. Once up and running, log into all servers and execute the remaining steps
on each instance.


3. Install git (git.x86_64 0:2.23.3-1.amzn2.0.1)

        sudo yum install https://centos7.iuscommunity.org/ius-release.rpm
        sudo yum install -y git


4. Install pip (pip-20.1.1 wheel-0.34.2)

        curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
        sudo python get-pip.py


5. Install miniconda (4.8.2)

        wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
        chmod 755 Miniconda3-latest-Linux-x86_64.sh
        sudo ./Miniconda3-latest-Linux-x86_64.sh -bu -p /emr/notebook-env


6. Install pypandoc

        sudo /usr/bin/pip install pypandoc
        sudo /usr/bin/pip3 install pypandoc


7. Install pyspark (py4j-0.10.7 pyspark-2.4.5)

        sudo /usr/bin/pip install pyspark
        sudo /usr/bin/pip3 install pyspark
        sudo /emr/notebook-env/bin/conda install pyspark


8. Install findspark (findspark-1.3.0-py_1)

        sudo /usr/bin/pip install findspark
        sudo /usr/bin/pip3 install findspark
        sudo /emr/notebook-env/bin/conda install findspark


9. Install sas-ssbdat plugin (spark-sas7bdat:2.1.0-s_2.11)

        /usr/bin/spark-shell --packages saurfang:spark-sas7bdat:2.1.0-s_2.11

        # exit scala with ctrl-c

        sudo /usr/bin/pip install sas7bdat
        sudo /usr/bin/pip3 install sas7bdat
        sudo /emr/notebook-env/bin/conda install sas7bdat


10. Install Parso (parso-0.7.0-pyh9f0ad1d_0)

        sudo /usr/bin/pip install parso
        sudo /usr/bin/pip3 install parso
        sudo /emr/notebook-env/bin/conda install parso


11. Install psycopg2 (psycopg2-binary-2.8.5)

        sudo /usr/bin/pip install psycopg2-binary
        sudo /usr/bin/pip3 install psycopg2-binary


12. Install pandas

        sudo /usr/bin/pip install pandas
        sudo /usr/bin/pip3 install pandas


13. Install boto3

        sudo /usr/bin/pip install boto3
        sudo /usr/bin/pip3 install boto3


14. Copy jar files to main jar lib

        sudo cp /home/hadoop/.ivy2/cache/saurfang/spark-sas7bdat/jars/spark-sas7bdat-2.1.0-s_2.11.jar /usr/lib/spark/jars/spark-sas7bdat-2.1.0-s_2.11.jar
        sudo cp /home/hadoop/.ivy2/cache/com.epam/parso/jars/parso-2.0.10.jar /usr/lib/spark/jars/parso-2.0.10.jar


15. File cleanup

        cd ~
        rm Miniconda3-latest-Linux-x86_64*
        rm get-pip.py


16. Pull down git repo (in home dir)

        cd ~
        git clone https://github.com/dodgerbluetx/DEND-Project-Capstone.git project
        cd ~/project


17. Configure aws cli, enter the following configuration

        aws configure

        AWS Access Key ID [None]: <enter access key id>
        AWS Secret Access Key [None]: <enter secret access key>
        Default region name [None]: us-west-2
        Default output format [None]:


18. Create dl.cfg file, and enter the following values.  Use your own S3 bucket
information.

        cd ~/project
        vim dl.cfg

        [AWS]
        AWS_ACCESS_KEY_ID = <access key id>
        AWS_SECRET_ACCESS_KEY = <secret access key>
        KEY = <access key id>
        SECRET = <secret access key>

        [CLUSTER]
        HOST = <cluster host name>
        DB_NAME = dwh
        DB_USER = awsuser
        DB_PASSWORD = <db user pwd>
        DB_PORT = 5439

        [IAM_ROLE]
        ARN = <iam role>

        [S3]
        INPUT_DATA = s3://ma2516-immigration-table/data
        CSV_DATA = s3://ma2516-immigration-data/

        [SAS]
        INPUT_SAS_DATA = s3a://ma2516-immigration-data/i94_feb16_sub.sas7bdat
        OUTPUT_SAS_DATA = s3a://ma2516-immigration-table/data

        [DWH]
        CLUSTER_TYPE = multi-node
        NUM_NODES = 2
        NODE_TYPE = dc2.large
        CLUSTER_IDENTIFIER = dwhCluster
        DB = dwh
        DB_USER = awsuser
        DB_PASSWORD = <db user pwd>
        PORT = 5439
        IAM_ROLE_NAME = dwhRole


19. Transfer sample to data to source s3 bucket (configured in Step 18)

        cd ~/project/data
        aws s3 cp i94port.csv s3://ma2516-immigration-data/i94port.csv
        aws s3 cp i94cnty.csv s3://ma2516-immigration-data/i94cnty.csv
        aws s3 cp i94mode.csv aws s3 cp s3://ma2516-immigration-data/i94state.csv i94state.csv
        aws s3 cp i94visacat.csv s3://ma2516-immigration-data/i94visacat.csv
        aws s3 cp i94visatype.csvs3://ma2516-immigration-data/i94visatype.csv
        aws s3 cp airlines_wiki.csv s3://ma2516-immigration-data/airlines_wiki.csv
        aws s3 cp airport-codes_csv.csv s3://ma2516-immigration-data/airport-codes_csv.csv
        aws s3 cp us-cities-demographics.csv s3://ma2516-immigration-data/us-cities-demographics.csv
        aws s3 cp i94_feb16_sub.sas7bdat s3://ma2516-immigration-data/i94_feb16_sub.sas7bdat


20. Run script to build Redshift cluster

        /usr/bin/python3 manage_aws.py --mode create_cluster


21. Run the script to create the Redshift table structure

        /usr/bin/python3 create_tables.py


22. Run the ETL

        /usr/bin/python3 etl.py


## Data Dictionary


Data Model ERD:

![](https://raw.githubusercontent.com/dodgerbluetx/DEND-Project-Capstone/master/images/dend-capstone-erd_02.png)


`immigration`

| Field Name  | Data Type | Description                              | Example    |
| ----------- | --------- | ---------------------------------------- | ---------- |
| id          | int       | Numeric identifier                       | 100        |
| i94yr       | int       | Four digit year                          | 2016       |
| i94mon      | int       | 1-2 digit month                          | 2          |
| i94cit_id   | int       | Numeric country identifier for arrival   | 209        |
| i94res_id   | int       | Numeric country identifier for residence | 210        |
| i94port_id  | varchar   | Three character port identifier          | DFW        |
| arrdate     | date      | Arrival date                             | DAL        |
| i94mode_id  | int       | Mode of arrival identifier               | 1          |
| i94addr_id  | varchar   | Two character state identifier           | TX         |
| depdate     | date      | Departure date                           | 2017-01-01 |
| i94bir      | int       | Age of respondant in years               | 41         |
| i94visa_id  | int       | Visa identifier                          | 2          |
| visapost    | varchar   | State Dept where Visa was issued         | MDR        |
| entdepa     | varchar   | Arrival/Parole flag                      | G          |
| entdepd     | varchar   | Departure/Death flag                     | O          |
| biryear     | int       | 4 digit birth year                       | 1979       |
| gender      | varchar   | Gender type                              | M          |
| airline_id  | varchar   | Airline used to arrive in the US         | AA         |
| fltno       | varchar   | Flight number used to arrive in the US   | 2169       |
| visatype_id | varchar   | Class of Visa admitted                   | WT         |


`ports`

| Field Name | Data Type | Description                        | Example |
| ---------- | --------- | ---------------------------------- | ------- |
| port_id    | varchar   | Three character airport identifier | DAL     |
| port_name  | varchar   | Port name                          | Dallas  |
| port_state | varchar   | Port state                         | TX      |


`countries`

| Field Name   | Data Type | Description                  | Example       |
| ------------ | --------- | ---------------------------- | ------------- |
| country_id   | int       | Numerical country identifier | 583           |
| country_name | varchar   | Country name                 | United States |


`modes`

| Field Name | Data Type | Description               | Example |
| ---------- | --------- | ------------------------- | ------- |
| mode_id    | int       | Numerical mode identifier | 1       |
| mode_name  | varchar   | Arrival mode type         | Air     |


`states`

| Field Name | Data Type | Description                    | Example    |
| ---------- | --------- | ------------------------------ | ---------- |
| state_id   | varchar   | Two character state identifier | CA         |
| state_name | varchar   | State name                     | CALIFORNIA |


`visa_categories`

| Field Name    | Data Type | Description               | Example  |
| ------------- | --------- | ------------------------- | -------- |
| visa_id       | int       | Numerical visa identifier | 1        |
| visa_category | varchar   | Visa category name        | Business |


`visa_types`

| Field Name   | Data Type | Description                       | Example  |
| ------------ | --------- | --------------------------------- | -------- |
| visa_type_id | varchar   | Alphanumeric visa type identifier | B1       |
| visa_type    | varchar   | Visa type name                    | Business |


`airlines`

| Field Name | Data Type | Description                     | Example               |
| ---------- | --------- | ------------------------------- | --------------------- |
| iata_id    | varchar   | Alphanumeric airline identifier | AA                    |
| icao       | varchar   | IACO identifier                 | AAL                   |
| name       | varchar   | Airline name                    | American Airlines     |
| call_sign  | varchar   | Call sign                       | AMERICAN              |
| country    | varchar   | Country of origin               | United States         |
| comment    | varchar   | Miscellaneous comment           | This is a description |


`demographics`

| Field Name         | Data Type | Description              | Example     |
| ------------------ | --------- | ------------------------ | ----------- |
| city               | varchar   | City name                | Los Angeles |
| state              | varchar   | State name               | California  |
| median_age         | float     | Median age               | 40.1        |
| male_population    | int       | Summary count            | 10000       |
| female_population  | int       | Summary count            | 10001       |
| total_population   | int       | Summary count            | 20001       |
| number_of_veterans | int       | Summary count            | 601         |
| foreign_born       | int       | Summary count            | 1200        |
| average_household  | float     | Average household size   | 2.61        |
| state_code         | varchar   | Two character state code | CA          |
| race               | varchar   | Race type                | Asian       |
| count              | int       | Summary count            | 13000       |


## Project Completion Write Up


The goal of this project was to demonstrate taking raw datasets, transforming
them by preparing and cleaning the data, and placing the data in a location that
can be access by others for various uses.  The key being using tools and various
processes/modeling techniques learned throughout the Data Engineering course.

Example queries for this data might be to aggregate the demographic data along 
with the immigration data to look for trends/correlations between them.
Airport data can also be joined to look for insights into more common ports of
entry for immigrants from different parts of the world.  The full i94 data set
for 2016 can be imported to look for peaks and valleys for immigration, and 
whether those high/low points map to any particular areas of the country.

I chose the model I implemented due to the elegance and simplicity
it offers. Spark integration was crucial to manage efficiently moving the data
between locations.

In a production scenario, the i94 data can be imported on a daily basis or as
it is made available. If I were to take this project to the next step, I would 
consider using Airflow for task orchestration.  Unfortunately, the workspaces 
provided do not offer easily integrating Spark and Airflow in the same ETL 
process.

If the data were to be increased by 100x, I would increase the amount of worker
nodes in the Spark EMR cluster for faster processing. Additionally I would 
increase the size of the Redshift cluster for faster data loading and querying.
That would also be a requirement for an environment where 100+ people are 
accessing the data.  However, I believe the current implementation I have built
can be scaled with the addition of more compute resources.
