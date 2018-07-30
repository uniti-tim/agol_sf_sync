# NOTE: You should only run this once to create the tables or if you decide to modify the table structure.
# This is not run continously on each instance of the script.

import os
from dotenv import *
import psycopg2 as pg
load_dotenv(find_dotenv()) #get by os.environ.get("VALUE")
try:
    conn = pg.connect(port=os.environ.get("DB_PORT"), host=os.environ.get("DB_HOST"), dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"), password=os.environ.get("DB_PASS"))
except:
    print "Unable to connect to DB - Exiting"
    exit()
cur = conn.cursor()

cur.execute("select * from information_schema.tables where table_name=%s", ('cpe_locations',))
if bool(cur.rowcount):
    print "DROPING TABLES..."
    cur.execute("DROP TABLE cpe_locations")

cur.execute("select * from information_schema.tables where table_name=%s", ('locations',))
if bool(cur.rowcount):
    cur.execute("DROP TABLE locations")

print "CREATING TABLES..."
cur.execute("""
CREATE TABLE cpe_locations (
ID text,
Latitude__c text,
Longitude__c text,
CPE_Location_Record__c text,
Name text,
Account__r_Name text,
Floor__c text,
Building__c text,
CPE_Location_Status__c text,
CPE_Location_Type__c text,
CPE_Location_Subtype__c text,
Site_ID__c text,
RecordType_Name text,
Location_Name__c text,
CreatedDate text,
CreatedBy_Name text,
LastModifiedDate text,
LastModifiedBy_Name text
)
""")


cur.execute("""
CREATE TABLE locations (
Id text,
LAT__c text,
LONG__c text,
Location_Record__c text,
Name text,
Svc_Address__c text,
Svc_City__c text,
Svc_State__c text,
Svc_Zip__c text,
Svc_County__c text,
Bldg_CLLI__c text,
Status__c text,
Location_Type__c text,
Location_Subtype__c text,
Location_Restrictions__c text,
Order_Restrictions__c text,
Site_Owner__c text,
CreatedDate text,
CreatedBy_Name text,
LastModifiedDate text,
LastModifiedBy_Name text
)
""")

conn.commit()
