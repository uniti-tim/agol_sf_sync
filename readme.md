## AGOL SF AUTO DEPLOY

To set this up you will need the following:
- Salesforce Credentials
- ArcGIS Online Organization Publishing Rights and Authorization
- Postgres 9.6+ DB Running

To assign credentials touch & edit the `.env` file.

Setup Posgresql DB to use for synchronization of the data to be stored. This is done to optimize speed and minimize calls to SF.

To Run this script you must have the following packages installed on your `PYTHON 2.7 || 3` instance.

```
arcgis
dotenv
simple_salesforce
psycopg2
```

This should then execute from its directory using `python sf_sync.py`
