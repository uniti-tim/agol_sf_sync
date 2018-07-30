#NOTE: This function takes the results that come from the single SF query and processes them into the PostGresql DB

def load_locations(sf,conn,cur):
    locations = sf.query("""
    SELECT Id,
    LAT__c,
    LONG__c,
    Location_Record__c,
    Name,
    Svc_Address__c,
    Svc_City__c,
    Svc_State__c,
    Svc_Zip__c,
    Svc_County__c,
    Bldg_CLLI__c,
    Status__c,
    Location_Type__c,
    Location_Subtype__c,
    Location_Restrictions__c,
    Order_Restrictions__c,
    Site_Owner__c,
    CreatedDate,
    CreatedBy.Name,
    LastModifiedDate,
    LastModifiedBy.Name
    FROM Location__c
    """)

    results = []

    for key,value in locations.items():
        if key == 'records':
            for record in value:
                results.append(record)

        if key == 'nextRecordsUrl':
            requires_more_queries = True
            url = value

            print("Need to get more results...")
            while requires_more_queries:
                print("Gathering Results From %s" % url)
                more_locations = sf.query_more(url, True)

                for record in more_locations['records']:
                    results.append(record)

                if 'nextRecordsUrl' in more_locations:
                    print("Need to Retrieve Even More Records")
                    url = more_locations['nextRecordsUrl']
                else:
                    print("Done Getting Records")
                    requires_more_queries = False

    print("There are %d records that are going to be initially loaded" % locations['totalSize'])

    print("--------------------------------------------------------------------------")

    print("Truncating locations Table")
    cur.execute("TRUNCATE TABLE locations RESTART IDENTITY")
    conn.commit()
    print("Table Truncated")

    print("--------------------------------------------------------------------------")

    print("Inserting Records")
    for record in results:
        stmt = '''INSERT INTO locations(\
        id, lat__c, long__c, location_record__c, name, svc_address__c,\
        svc_city__c, svc_state__c, svc_zip__c, svc_county__c, bldg_clli__c, \
        status__c, location_type__c, location_subtype__c, location_restrictions__c, \
        order_restrictions__c, site_owner__c, createddate, createdby_name, \
        lastmodifieddate, lastmodifiedby_name)
    	VALUES (\
        '{id}', '{lat__c}', '{long__c}', '{location_record__c}', '{name}', '{svc_address__c}',\
        '{svc_city__c}', '{svc_state__c}', '{svc_zip__c}', '{svc_county__c}', '{bldg_clli__c}', \
        '{status__c}', '{location_type__c}', '{location_subtype__c}', '{location_restrictions__c}', \
        '{order_restrictions__c}', '{site_owner__c}', '{createddate}', '{createdby_name}', \
        '{lastmodifieddate}', '{lastmodifiedby_name}')'''.format(
        id = record['Id'],
        lat__c = record['LAT__c'].replace("'","") if record['LAT__c'] is not None else "NULL",
        long__c = record['LONG__c'].replace("'","") if record['LONG__c'] is not None else "NULL",
        location_record__c = record['Location_Record__c'].replace("'",'') if record['Location_Record__c'] is not None else "NULL",
        name = record['Name'].replace("'",'').replace('&','') if record['Name'] is not None else "NULL",
        svc_address__c = record['Svc_Address__c'].replace("'",'') if record['Svc_Address__c'] is not None else "NULL",
        svc_city__c = record['Svc_City__c'].replace("'",'') if record['Svc_City__c'] is not None else 'NULL',
        svc_state__c = record['Svc_State__c'] if record['Svc_State__c'] is not None else "NULL",
        svc_zip__c = record['Svc_Zip__c'] if record['Svc_Zip__c'] is not None else "NULL",
        svc_county__c = record['Svc_County__c'].replace("'",'') if record['Svc_County__c'] is not None else "NULL",
        bldg_clli__c = record['Bldg_CLLI__c'].replace("'",'') if record['Bldg_CLLI__c'] is not None else "NULL",
        status__c = record['Status__c'] if record['Status__c'] is not None else "NULL",
        location_type__c = record['Location_Type__c'] if record['Location_Type__c'] is not None else "NULL",
        location_subtype__c = record['Location_Subtype__c'] if record['Location_Subtype__c'] is not None else "NULL",
        location_restrictions__c = record['Location_Restrictions__c'] if record['Location_Restrictions__c'] is not None else "NULL",
        order_restrictions__c = record['Order_Restrictions__c'] if record['Order_Restrictions__c'] is not None else "NULL",
        site_owner__c = record['Site_Owner__c'].replace("'",'') if record['Site_Owner__c'] is not None else "NULL",
        createddate = record['CreatedDate'] if record['CreatedDate'] is not None else "NULL",
        createdby_name = record['CreatedBy']['Name'].replace("'",'') if record['CreatedBy'] is not None and record['CreatedBy']['Name'] is not None else "NULL" ,
        lastmodifieddate = record['LastModifiedDate'] if record['LastModifiedDate'] is not None else "NULL" ,
        lastmodifiedby_name = record['LastModifiedBy']['Name'].replace("'",'') if record['LastModifiedBy'] is not None and record['LastModifiedBy']['Name'] is not None else "NULL" ,
        )
        cur.execute(stmt)
        conn.commit()

    print("Locations have been Synchronized.")
    print("--------------------------------------------------------------------------")
