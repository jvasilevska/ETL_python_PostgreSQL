# Python exercise

## Requirements

### Part 1 
Using Python. 
Take the following files, create database structure for them and load them into PostgreSQL. 
Whether you normalize the data is up to you, but both header and record data must be stored.  
The structure is documented at: 
* https://www1.ncdc.noaa.gov/pub/data/igra/data/igra2-data-format.txt 
* https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070261-data.txt.zip 
* https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070219-data.txt.zip 
* https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070361-data.txt.zip
* https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070308-data.txt.zip 
* https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070398-data.txt.zip 
### Part 2 
Connect to Postgres, digest the data, and write CSVt files. 
The CSV files should include all data for the weather balloons, and be partitioned by thousands of meters altitude (i.e. partitioning to 0-1000, 1001-2000, 2001-3000, etc). 

You may create multiple files per partition but each file should only contain data for one partition. 

## Installation

### Requirements
* Python 3
* PostgreSQL
* virtualenv (optional)

### Setup

* Clone the repository  
`git clone git@github.com:jvasilevska/python_exercise.git`

* Add your credentials for database connection in config.ini

* With virtualenv:  
    * Create new environment: `python3 -m venv env`
    * Activate the environement: `source env/bin/activate`
    * Install requirements: `pip install -r requirements.txt`
    * When done, deactivate environment: `deactivate`

* Without virtualenv:
    * Install requirements: `pip install -r requirements.txt`
    
## Running the script
`python task.py`



