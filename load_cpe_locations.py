#NOTE: This function takes the results that come from the single SF query and processes them into the PostGresql DB

def load_cpe_locations(sf,conn,cur):
    cpe_locations = sf.query("""
    SELECT ID,
    Latitude__c,
    Longitude__c,
    CPE_Location_Record__c,
    Name,
    Account__r.Name,
    Floor__c,
    Building__c,
    CPE_Location_Status__c,
    CPE_Location_Type__c,
    CPE_Location_Subtype__c,
    Site_ID__c,
    RecordType.Name,
    Location_Name__c,
    CreatedDate,
    CreatedBy.Name,
    LastModifiedDate,
    LastModifiedBy.Name
    FROM CPE_Location__c
    """)

    results = []

    for key,value in cpe_locations.items():
        if key == 'records':
            for record in value:
                results.append(record)

        if key == 'nextRecordsUrl':
            requires_more_queries = True
            url = value

            print("Getting more results...")
            while requires_more_queries:
                print("Gathering Results From %s" % url)
                more_locations = sf.query_more(url, True)

                for record in more_locations['records']:
                    results.append(record)

                if 'nextRecordsUrl' in more_locations:
                    print("Need to Retrieve More Records")
                    url = more_locations['nextRecordsUrl']
                else:
                    print("Done Getting Records")
                    requires_more_queries = False

    print("There are %d records that are going to be initially loaded" % cpe_locations['totalSize'])

    print("--------------------------------------------------------------------------")

    print("Truncating cpe_locations Table")
    cur.execute("TRUNCATE TABLE cpe_locations RESTART IDENTITY")
    conn.commit()
    print("Table Truncated")

    print("--------------------------------------------------------------------------")

    print("Inserting Records")
    for record in results:
        stmt = '''INSERT INTO cpe_locations(\
        id, latitude__c, longitude__c, cpe_location_record__c, name,\
        account__r_name, floor__c, building__c, cpe_location_status__c,\
        cpe_location_type__c, cpe_location_subtype__c,\
        site_id__c, recordtype_name, location_name__c, createddate, createdby_name,\
        lastmodifieddate, lastmodifiedby_name)\
    	VALUES (\
        '{id}', '{latitude__c}', '{longitude__c}', '{cpe_location_record__c}', '{name}',\
        '{account__r_name}', '{floor__c}', '{building__c}', '{cpe_location_status__c}',\
        '{cpe_location_type__c}', '{cpe_location_subtype__c}',\
        '{site_id__c}', '{recordtype_name}', '{location_name__c}', '{createddate}', '{createdby_name}',\
        '{lastmodifieddate}', '{lastmodifiedby_name}')'''.format(
        id = record['Id'] if record['Id'] is not None else "NULL",
        latitude__c = record['Latitude__c'].replace("'",' ') if record['Latitude__c'] is not None else "NULL",
        longitude__c = record['Longitude__c'].replace("'",' ') if record['Longitude__c'] is not None else "NULL",
        cpe_location_record__c = record['CPE_Location_Record__c'].replace("'",'').replace("&",'') if record['CPE_Location_Record__c'] is not None else "NULL",
        name = record['Name'].replace("'",'').replace("&",'') if record['Name'] is not None else "NULL",
        account__r_name = record['Account__r']['Name'].replace("'",'').replace("&",'') if record['Account__r'] is not None and record['Account__r']['Name'] is not None else "NULL" ,
        floor__c = record['Floor__c'].replace("'",'').replace("&",'') if record['Floor__c'] is not None else "NULL",
        building__c = record['Building__c'].replace("'",'').replace("&",'') if record['Building__c'] is not None else "NULL",
        cpe_location_status__c =  record['CPE_Location_Status__c'] if record['CPE_Location_Status__c'] is not None else "NULL",
        cpe_location_type__c =record['CPE_Location_Type__c'] if record['CPE_Location_Type__c'] is not None else "NULL",
        cpe_location_subtype__c =record['CPE_Location_Subtype__c'] if record['CPE_Location_Subtype__c'] is not None else "NULL",
        site_id__c = record['Site_ID__c'].replace("'",'').replace("&",'') if record['Site_ID__c'] is not None else "NULL",
        recordtype_name = record['RecordType']['Name'].replace("'",'').replace("&",'') if record['RecordType'] is not None and record['RecordType']['Name'] is not None else "NULL",
        location_name__c = record["Location_Name__c"] if record['Location_Name__c'] is not None else "NULL",
        createddate = record['CreatedDate'] if record['CreatedDate'] is not None else "NULL",
        createdby_name = record['CreatedBy']['Name'].replace("'",'').replace("&",'') if record['CreatedBy'] is not None and record['CreatedBy']['Name'] is not None else "NULL",
        lastmodifieddate = record['LastModifiedDate'] if record['LastModifiedDate'] is not None else "NULL",
        lastmodifiedby_name = record['LastModifiedBy']['Name'].replace("'",'').replace("&",'') if record['LastModifiedBy'] is not None and record['LastModifiedBy']['Name'] is not None else "NULL",
        )
        cur.execute(stmt)
        conn.commit()

    print("CPE Locations have been Synchronized.")
    print("--------------------------------------------------------------------------")
