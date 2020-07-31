#!/opt/csw/bin/python2.7
# Transfer_table_data.py, TAS, TimeSeries, 02/18/2014
# Script to retrieve and upload NERRS data to hosted Natweb database.
# Written by Todd Siskin, US Geological Survey

# Version: 1.01

# Modifications
# Date        User      Modification
# 02/14/2014  Tsiskin   Original Coding from individual tasks.
# 04/03/2019  Tsiskin   Added command line processing for back-filling by date.
#                        Merged code for database writing.
# 04/04/2019  Tsiskin   Added sys call to include libdir for credentialing with cron.
#                        Fixed RecordExists to check for correctly formatted date
#                        and check parameter. Date only causes some parameters to be ignored.
# 04/08/2019  Tsiskin   Added --list to display found sites. Changed --save to --site to
#                        change SiteID from default (hud). Added --verbose to display intermediate
#                        collection information and processing steps. Added DB commit to loop.
# 04/09/2019  Tsiskin   Reverse the processed array of data before writing to database.
#                        Change tz_code to EST to reflect saved timestamp.
#                        Added logging routine for various conditions.
# 04/30/2019  Tsiskin   Added a replace on key to remove extraneous slash that shows up from 
#                        time to time. Not consistent.
# 05/15/2019  Tsiskin   Added a new station as requested by GRWall.
# 06/14/2019  Tsiskin   Reworked to read from post table and write to NERRS table.
#                        Most NERRS soap code removed. Removed call to RecordExists. This was
#                        causing the program to take days to run. Added test for first known date 
#                        in auto-collect database as breaking point in copy to avoid duplication.
#                        Added milestone markers to show progress and commit changes.
#                        Added translation of parameter names between tables. 

# Global definitions

ONEHOUR = 60*60
ONEDAY = ONEHOUR*24
FIVEMIN = 60*5
DATAPATH = "backend/data/well_logs.subf"
Test = False

# Load System modules
import os, sys, time, datetime, string, getopt
import urllib, ssl
import MySQLdb
import re

# Functions

def grep(list, srch, indx=None):
    gindx = 0
    for item in list:
        if item.find(srch) != -1:
            if indx:
                return gindx
            else:
                return item
        gindx += 1

    if indx:
       return -1
    else:
       raise IndexError

def findall(list, srch, buffer):
   for item in list:
      if item.find(srch) != -1:
         buffer.append(item)
   return buffer

def is_numeric(literal):
   """Return whether a literal can be parsed as a numeric value"""
   castings = [int, float, complex,
      lambda s: int(s,2),  #binary
      lambda s: int(s,8),  #octal
      lambda s: int(s,16)] #hex
   for cast in castings:
      try:
         cast(literal)
         return True
      except ValueError:
         pass
   return False

# SaveBuffer(data array, station ID, database, data source, date/time stamp)
# Save the supplied data array to allow for tracking of web information over time.

def SaveBuffer(data, stn, db, src, dts):
   bDir = "./tmp"
   fname = "%s-%s_%s-%s.txt" % (stn, db, src, dts)

   if data == None:
      data = [ "No Data Available" ]

   bFD = open(os.path.join(bDir, fname), "w")
   for line in data:
      bFD.write(line + '\n')
   bFD.close()

# RecordExists(RE_table, RE_date, RE_siteid)
# Check to see if a site record already exists to avoid duplication.

def RecordExists(RE_tbl, RE_dt, RE_sid, RE_par):
   try:
      RE_SQLCmd = "SELECT id FROM %s WHERE date_time='%s' AND site_name='%s' and parameter='%s'" % (RE_tbl, RE_dt, RE_sid, RE_par)
      HstCur.execute(RE_SQLCmd)
   except MySQLdb.Error as why:
      print ("An error has occurred. (RecordExists) %s" % why)
      return False

   if (HstCur.rowcount != 0):
      return True
   else:
      return False

# RefreshDBConn(Connection, Cursor)
# Check connection and refresh if it was lost due to excessive delays.

def RefreshDBConn(RDBC_Conn, RDBC_Cur):
   try:
      RDBC_Conn.commit()
   except:
      RDBC_Conn = MySQLdb.connect(host=HstHostname, port=HstMySQLPort, user=HstUsername, \
         passwd=HstPasswd, db=HstDatabasename)
      RDBC_Cur = RDBC_Conn.cursor(MySQLdb.cursors.DictCursor)

   return RDBC_Conn, RDBC_Cur

def NotifyAdmin(fname, code):
   if code == 1:
      logmsg = "Possible error in data stream %s. Please check" % fname
   if code == 2:
      logmsg = "No entries found for site %s." % fname
   if code == 3:
      logmsg = "Current data not available for site %s" % fname
   if code == 4:
      logmsg = "No data retrieved for site %s" % fname

   dtm = time.strftime("%b %d %H:%M:%S", time.localtime(time.time()))
   msg = "%s NERRS %s\n" % (dtm, logmsg)
   open(os.path.join(DepotDir, "log", "Get_NERRS_Data.log"), 'a').write(msg)

   return

#converted/updated version of code from Marty's aq-test
def parseRDB(data):

   comments = True
   definitions = True
   data = re.split("\r?\n", data)
   results = []
   headers = []
   #for (var i = 0; i < data.length; i++) {
   for i, line in enumerate(data):

      #make sure there is something on the line
      if len(line) > 0:

         #skip comment rows
         if line[0] == '#':
            continue
         #next row is column headings
         elif comments:
            comments = False
            headers = re.split("\t", line)
         #next row are unneeded data definitions
         elif not comments and definitions:
            definitions = False
         #finally we are at the data rows
         elif not comments and not definitions:
            row = re.split("\t", line)

            lineData = {}
            for i, header in enumerate(headers):
               if i < len(row) and len(row[i]) > 0:
                  lineData[header] = row[i]
               else:
                  lineData[header] = None
            results.append(lineData);
   
   return results

def TryParseFloat (x):
   if x is None:
      return None
   try:
      return float(x)
   except:
      return None

# End Functions
# Begin main program
with open(DATAPATH, "r") as myfile:
    rawData=myfile.read()
data = parseRDB(rawData)

# Set local definitions
LibDir = "/usr/local/local"
DepotDir = "/usr/opt/cron_depot"

# adjust the module search path so credentialing files can be found
sys.path.insert(1, os.path.join(LibDir, "lib"))

# NatWeb Hosted Database server credentials
from DB_Connect_wellviewer import *

# Main program 

# Turn off certificate verification for https
ssl._create_default_https_context = ssl._create_unverified_context

# Make connection to Hosted Database Server (MySQL)
try:
   db_conn = MySQLdb.connect(host=HstHostname, port=HstMySQLPort, user=HstUsername, \
      passwd=HstPasswd, db=HstDatabasename)
   HstCur = db_conn.cursor(MySQLdb.cursors.DictCursor)
except MySQLdb.MySQLError as why:
   print (why[1])
   Test = True
   if why[0] == 2003:
      sys.exit(why[0])
except KeyError as why:
   print (why[1])
   Test = True

if Test:
   print ("Collected %d total records." % len(data))

db_conn, HstCur = RefreshDBConn(db_conn, HstCur)

Cnt = 0
dupCount = 0
for line in data:
   site_id = line["C001"]
   loc_well_num =  line["C012"]
   altitude = TryParseFloat(line["C016"])
   altitude_datum = line["C022"]
   well_depth = TryParseFloat(line["C028"])
   depth_to_interval = TryParseFloat(line["C091"])
   aquifer_cd = line["C093"]
   lith_cd = line["C096"]
   mat_description = line["C097"]
   casing_depth = TryParseFloat(line["C078"])
   remark = line["C185"]
   if line["C910"] is None or line["C909"] is None:
      point = 'Point(-1, -1)'
   else:
      point = 'Point(%s, %s)' % (line["C910"], line["C909"])
   
   SQLCmd = "INSERT INTO WELLS(SITE_ID, LOC_WELL_NUM, POS, ALTITUDE, ALTITUDE_DATUM, WELL_DEPTH, DEPTH_TO_INTERVAL, AQUIFER_CD, LITH_CD, MAT_DESCRIPTION, CASING_DEPTH, REMARK) VALUES(%s, %s, " + point + ", %s, %s, %s, %s, %s, %s, %s, %s, %s)"
   
   # \
     # % (line["C001"], line["C012"], point, line["C016"], line["C022"], line["C028"], line["C091"], line["C093"], line["C096"], line["C097"], line["C078"], line["C185"])
   if Test:
      print (SQLCmd % (site_id, loc_well_num, altitude, altitude_datum, well_depth, depth_to_interval, aquifer_cd, lith_cd, mat_description, casing_depth, remark))
   else:
      try:
         Cnt += 1
         HstCur.execute(SQLCmd, (site_id, loc_well_num, altitude, altitude_datum, well_depth, depth_to_interval, aquifer_cd, lith_cd, mat_description, casing_depth, remark))
      except KeyboardInterrupt:
         db_conn.commit()
         sys.exit()
      except MySQLdb.ProgrammingError as err:
         print("Check your syntax: \n %s \n" % (HstCur._last_executed))
      except MySQLdb.IntegrityError as err:
         dupCount += 1
         #print ("Likely duplicate entry for %s \n %s \n" % (line["C001"], HstCur._last_executed))
         continue
   if (Cnt % 200) == 0:
      #if Test:
      print ("Processed %d records. Found %d duplicates" % (Cnt, dupCount))
      if not Test:
         db_conn.commit()

if not Test:
   db_conn.commit()
   db_conn.close()
sys.exit()
