Some notes on the code here:


BACKEND:


LogInterpreter.py:
This contains some general functionality that should be converted to javascript and put on the frontend. This uses Regex to parse out well logs and check if arbitrary text strings are of 
the same format as a well log. 


GetWellData.py:
This was mostly experiments with loading the subf files. These deemed it was too slow. This file isn't that useful. 

FRONTEND:

app.js:
This is a lightly modified version of the template. It has the functionality to take a formatted json (similar to what LogInterpreter can create), and turn it into the appropriate html to display
a well log as a column with material textures. There are a few functions in here that do fuzzy text matching etc. 


DATABASE:

The database is setup already:

$db_hostname = "mysql.natweb.usgs.gov";
$db_database = "wellviewerdb";
$db_username = "wellviewer";
$db_password = "px8XELSV7h3X";

There is a file in the backend/util folder (transfer_data_table.py) that reads a subf file and loads it into the database using the info above found in DB_Connect_wellviewer.py.

This may need to be fixed to correct the issue of multiple site IDs in the same file (suggesting siteID is not the primary key of that data..). Transfer_data_table will be what gets run by the CRON job.

The database is setup to be spatially indexed to the position column. This means that geometry queries should be really fast which will help make loading sites visible on the map easy.

It should be possible to do queries that check if wells are within a certain polygon, for example.