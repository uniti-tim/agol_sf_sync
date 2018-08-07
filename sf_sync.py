import os, time
from dotenv import *
from simple_salesforce import Salesforce
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection, FeatureLayer
import psycopg2 as pg
import requests
load_dotenv(find_dotenv()) #get by os.environ.get("VALUE")

#custom imports
from load_locations import load_locations
from load_cpe_locations import load_cpe_locations
from create_datafile import create_datafile

# establish connection objects to SF and to DB
sf = Salesforce(username=os.environ.get("SF_USERNAME"), password=os.environ.get("SF_PASSWORD"), security_token=os.environ.get("SF_TOKEN"))
try:
    conn = pg.connect(port=os.environ.get("DB_PORT"), host=os.environ.get("DB_HOST"), dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"), password=os.environ.get("DB_PASS"))
except:
    print("Unable to connect to DB - Exiting")
    exit()
cur = conn.cursor()
gis = GIS(os.environ.get("AGOL_SERVICE_URL"),os.environ.get("AGOL_USER"), os.environ.get("AGOL_PASS"))

# Get and Push into Truncated Tables - totally replaces all data in both locations and cpe_locations table
load_locations(sf,conn,cur)
load_cpe_locations(sf,conn,cur)
# should a function be here that cleansup the NULL Lat/lngs after loading?

create_datafile(conn,cur)


data_name = 'Uniti_Salesforce_Locations'
locations_csv = gis.content.search(data_name,'CSV')
description = "Locations with or without CPEs. Exported from Salesforce. If the Location does not have a CPE, \
    the Location will still be included and the CPE fields will be blank. \
    If there are multiple CPEs at a location, that location will appear for each CPE. \
    Excludes 'Virtual' LOCs and CPEs. \
    This Service was updated on %s by an automated tool. \
    Should the data be corrupt or there is an obvious issues with the tool's output contact the \
    script creator timothy.carambat@uniti.com or feature owner rich.thomas@uniti.com." %(time.ctime())

if len(locations_csv) > 0:
    locations_csv[0].update(data='locations.csv')
    print("Locations CSV Updated with ID: %s" % (locations_csv[0].id))
    csv_item = locations_csv[0]
    csv_item.update(item_properties={'description': description})
else:
    item_prop = {
     'title':data_name,
     'description': description
     }
    gis.content.add(item_properties=item_prop, data='locations.csv')
    while True:
        if len(gis.content.search(data_name,'CSV')) > 0:
            csv_item = gis.content.search(data_name,'CSV')[0]
            break;

    print("Location CSV Uploaded With ID: %s" % (csv_item.id) )



if len(gis.content.search(data_name,'Feature Layer')) == 0:
    csv_item.publish(
    overwrite=True,
    publish_parameters={
        'itemID': csv_item.id,
        'type': 'csv',
        'name': data_name,
        'locationType': 'coordinates',
        'latitudeFieldName': 'latitude_loc',
        'longitudeFieldName': 'longitude_loc',
        'description': description,
    }
    )

    while True:
        if len(gis.content.search(data_name,'Feature Layer')) > 0:
            locations_new_fl = gis.content.search(data_name,'Feature Layer')[0]
            break;

    print("Location Feature Layer Uploaded With ID: %s" % (locations_new_fl.id) )

else:
    locations_new_fl = gis.content.search(data_name,'Feature Layer')[0]
    print("Location Feature Layer Already Exists with ID: %s" % (locations_new_fl.id))

    fl_layer = FeatureLayer.fromitem(locations_new_fl)
    fl_layer.manager.truncate()
    print("Location Data Truncated...")

    FeatureLayerCollection.fromitem(locations_new_fl).manager.overwrite('locations.csv')

locations_new_fl.share(everyone=False, org=True, groups=['CPE'], allow_members_to_edit=False)
print("New Location Layer Shared with CPE Group")

locations_new_fl.update(thumbnail=r'thumbnail.jpg')
print("Thumbnail Set")

print("Item Sucessfully Published!")
os.remove("locations.csv")
print("CSV removed")
