# Import statements
import psycopg2
import sys
import psycopg2.extras
import csv
import json

from creds import *

# Global variables
db_connection = None
db_cursor = None
stateList = ['Arkansas', 'California', 'Michigan']

# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
	global db_connection, db_cursor
	if not db_connection:
		try:
			db_connection = psycopg2.connect("dbname='{}' user = '{}' password = '{}'".format(db_name, db_user, db_password))
			print("Connected to database {} as {}.".format(db_name, db_user))
		except:
			print("Unable to connect to database {}.".format(db_name))
			sys.exit(1)

	if not db_cursor:
		db_cursor = db_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

	return db_connection, db_cursor


# Write code / functions to create tables with the columns you want and all database setup here.
def makeTables():

	print("\n*************** Making tables ***************")
	
	try:
		cur.execute(""" DROP TABLE IF EXISTS states, sites CASCADE """)
		conn.rollback()
		print("     - Database cleared (tables dropped)")
	except:
		print("     * Could not clear database.")

	try:
		cur.execute(""" CREATE TABLE IF NOT EXISTS states(ID SERIAL PRIMARY KEY, Name VARCHAR(40) UNIQUE) """)
		print("     - Table 'states' created successfully.")
	except:
		print("     * Could not create table 'states'")

	try:
		cur.execute(""" CREATE TABLE IF NOT EXISTS sites(ID SERIAL PRIMARY KEY, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER, Location VARCHAR(128), Description TEXT, FOREIGN KEY(State_ID) REFERENCES states(ID)) """)
		print("     - Table 'sites' created successfully.")
	except:
		print("     * Could not create table 'sites'")

	print("*********************************************")

# Write code / functions to deal with CSV files and insert data into the database here.
def getData(fileName):
	result = []
	with open(fileName, mode = 'r', encoding = 'utf8', newline = '') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			rowDict = {}
			rowDict['NAME'] = row['NAME']
			rowDict['TYPE'] = row['TYPE']
			rowDict['LOCATION'] = row['LOCATION']
			rowDict['DESCRIPTION'] = row['DESCRIPTION']
			result.append(rowDict)
	return(result)

def insertStates(state, conn, cur):
	sql = """ INSERT INTO States(Name) VALUES(%s) """
	cur.execute(sql,[state])
	
	return(True)

def insertSites(siteDxn, state, conn, cur):
	sql = """ INSERT INTO Sites(Name, Type, State_ID, Location, Description) VALUES(%s, %s, (SELECT ID FROM States WHERE Name = %s), %s, %s) """
	cur.execute(sql,(siteDxn["NAME"], siteDxn["TYPE"], state, siteDxn["LOCATION"], siteDxn["DESCRIPTION"]))

	return(True)

# Write code to be invoked here (e.g. invoking any functions you wrote above)
conn, cur = get_connection_and_cursor()
makeTables()

mi = getData("michigan.csv")
ak = getData("arkansas.csv")
ca = getData("california.csv")

print("\n*************** Populating 'States' table ***************")
for state in stateList:
	try:
		res = insertStates(state, conn, cur)
		if res:
			print("     - {} added to database".format(state))
		else:
			print("     * Failed to add {} to database".format(state))
	except:
		pass
print("Done.")
print("**********************************************************")

print("\n*************** Populating 'Sites' table ***************")
for sites in ak:
	try:
		res = insertSites(sites, "Arkansas", conn, cur)
		if res:
			print("     - {} added to database".format(sites["NAME"]))
		else:
			print("     * Failed to add {} to database".format(sites["NAME"]))
	except:
		pass

for sites in ca:
	try:
		res = insertSites(sites, "California", conn, cur)
		if res:
			print("     - {} added to database".format(sites["NAME"]))
		else:
			print("     * Failed to add {} to database".format(sites["NAME"]))

	except:
		pass

for sites in mi:
	try:
		res = insertSites(sites, "Michigan", conn, cur)
		if res:
			print("     - {} added to database".format(sites["NAME"]))
		else:
			print("     * Failed to add {} to database".format(sites["NAME"]))

	except:
		pass
print("Done.")
print("**********************************************************")

conn.commit()

# Write code to make queries and save data in variables here.
cur.execute(""" SELECT Location FROM Sites """)
all_locations = cur.fetchall()

cur.execute(""" SELECT Name FROM Sites WHERE Description LIKE '%beautiful%' """)
beautiful_sites = cur.fetchall()

cur.execute(""" SELECT COUNT(ID) FROM Sites WHERE Type='National Lakeshore' """)
natl_lakeshores = cur.fetchall()

cur.execute(""" SELECT Sites.Name FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name = 'Michigan' """)
michigan_names = cur.fetchall()

cur.execute(""" SELECT COUNT(Sites.ID) FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name = 'Arkansas' """)
total_number_arkansas = cur.fetchall()
print(total_number_arkansas)