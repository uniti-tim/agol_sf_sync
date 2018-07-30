# NOTE: This query should get all locations and their associated CPE IF they have one. If they dont - all null.
# This file is querying the PostgresDB directly. NOT Salesforce.

def create_datafile(conn, cur):
    print("Querying Data...")

    data_query = """
    SELECT
    location.id as Location_Salesforce_Internal_ID,
    location.lat__c as Latitude_LOC,
    location.long__c as Longitude_LOC,
    location.location_record__c as Location_Record,
    location.name as Location_Name,
    location.svc_address__c as Svc_Address,
    location.svc_city__c as Svc_City,
    location.svc_state__c as Svc_State,
    location.svc_zip__c as Svc_Zip,
    location.svc_county__c as Svc_County,
    location.bldg_clli__c as Building_CLLI,
    location.status__c as Location_Status,
    location.location_type__c as Location_Type,
    location.location_subtype__c as Location_Subtype,
    location.location_restrictions__c as Location_Restrictions,
    location.order_restrictions__c as Order_Restrictions,
    location.site_owner__c as LOC_Site_Owner,
    location.createddate as LOC_Created_Date,
    location.createdby_name as LOC_Created_By,
    location.lastmodifieddate as LOC_Last_Modified_Date,
    location.lastmodifiedby_name as LOC_Last_Modified_By,
    cpe_location.id as CPE_Salesforce_Internal_ID,
    cpe_location.latitude__c as Latitude_CPE,
    cpe_location.longitude__c as Longitude_CPE,
    cpe_location.cpe_location_record__c as CPE_Record,
    cpe_location.name as CPE_Name,
    cpe_location.account__r_name as CPE_Account_Name,
    cpe_location.floor__c as Floor,
    cpe_location.building__c as Building,
    cpe_location.cpe_location_status__c as CPE_Location_Status,
    cpe_location.cpe_location_type__c as CPE_Location_Type,
    cpe_location.cpe_location_subtype__c as CPE_Location_Subtype,
    cpe_location.site_id__c as Customer_Site_ID,
    cpe_location.recordtype_name as CPE_Record_Type,
    cpe_location.createddate as CPE_Created_Date,
    cpe_location.createdby_name as CPE_Created_By,
    cpe_location.lastmodifieddate as CPE_Last_Modified_Date,
    cpe_location.lastmodifiedby_name as CPE_Last_Modified_By
    FROM locations as location
    JOIN cpe_locations as cpe_location ON location.id = cpe_location.location_name__c WHERE cpe_location.cpe_location_type__c <> 'Virtual'
    """

    # This will output the entire results into a single csv without having to explictly define a loop
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(data_query)

    print("Data Collected - Generating CSV")

    #create our file to use for digestion later
    with open('locations.csv', 'w',encoding="utf-8") as f:
        cur.copy_expert(outputquery.encode('utf8'), f)
        f.close()

    print("CSV Created locations.csv")
