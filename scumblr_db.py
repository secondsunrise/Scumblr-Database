import json
import sqlite3
import os
import sys
import time

"""
Table Info for results

0|id|INTEGER|1||1
1|title|varchar(255)|0||0
2|url|varchar(255)|0||0
3|status_id|integer|0||0
4|created_at|datetime|0||0
5|updated_at|datetime|0||0
6|domain|varchar(255)|0||0
7|user_id|integer|0||0
8|content|text|0||0
9|metadata|text|0||0
"""

while True:

    # How often should the JSON file be updated (in seconds)?
    update_interval = 60

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

	# Get the last ID from the JSON file
    def get_last_id(data):
        last_id = None
        for i in range(0, len(data)):
            if data[i]["id"] > last_id:
                last_id = data[i]["id"]
        return last_id

	# Connect to the database
    def db_connect():
        try:
            print "Starting database connection..."
            print "-----"
            connection = sqlite3.connect("/your_path_to/Scumblr/db/development.sqlite3")
            connection.row_factory = dict_factory
            cursor = connection.cursor()
            return connection, cursor
        except:
            print "Unexpected error connecting to the database: ", sys.exc_info()[0]

    # Start the databse connection and cursor
    connection, cursor = db_connect()

    # Name of JSON file where database queries will be stored
    json_file = "scumblr_db"
    json_file += ".json"

    # If the JSON file exists AND has data in it already:
    if os.path.isfile('./' + json_file) and os.stat(json_file).st_size > 0:

        print "JSON file exists AND is populated with data"

        # Read in the data already in the JSON file
        print "Loading data from file..."
        with open(json_file, mode='r') as jfile:
            data_in_file = json.load(jfile) # <type 'list'>

        # Determine what data needs to be added to the file (everything after the last found id)
        last_id = get_last_id(data_in_file)
        print "Last id in file: ", last_id

        # Get the data from the database that needs to be added to the JSON file
        print "Retrieving the necessary data..."#, last_id
        cursor.execute("SELECT id, title, domain, url, created_at "
                       "FROM results "
                       "WHERE id > %s" % (last_id))
        needed_results = cursor.fetchall()

        # If there are needed results, then add then to the JSON file
        if len(needed_results) > 0:

            print len(needed_results), " results retrieved" # : , needed_results
            print "Updating file..."

            # Append the data that was pulled from the database to the JSON file
            for item in range(0, len(needed_results)):
                data_in_file.append(json.loads(json.dumps(needed_results[item])))

            # Write the added results to the JSON file
            with open(json_file, mode='w') as jfile:
                jfile.write(json.dumps(data_in_file, indent=4))

            print "File updated"

        # Else If there are no needed results, stop the script and close the databse connection
        elif len(needed_results) == 0:
            print len(needed_results), " results retrieved, file is up to date"
            print "Exiting script..."

    # Else If the JSON file does not exist OR exists but is empty:
    elif not os.path.isfile('./' + json_file) or os.stat(json_file).st_size == 0:

        print "JSON file does not exists OR is empty"

        # Retrieve all the data from the database
        print "Retrieving all data from the database..."
        cursor.execute("SELECT id, title, domain, url, created_at "
                       "FROM results")
        all_data = cursor.fetchall()

        # Write all data to the JSON file
        print "Populating JSON file with data..."
        with open(json_file, mode='w') as jfile:
            jfile.write(json.dumps(all_data, indent = 4))
        print "JSON file successfully populated"

    print "-----"
    print "Closing databse connection..."
    print ""
    connection.close()

    time.sleep(update_interval)
