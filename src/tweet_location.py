# Change location format to support Geohashing in Kibana

import couchdb

couchdb_locs = ['http://localhost:5984']
database_name = 'crime_tweets'

def change_location(couchdb_loc,database_name):
	couch= couchdb.Server(couchdb_loc)
	db = couch[database_name]
	lat_field = 'latitude'
	lon_field = 'longitude'
	loc_field = 'location'
	count = 0;
	for id in db:
		doc = db[id]
		if (loc_field in doc):
			if (lat_field in doc[loc_field]) and (lon_field in doc[loc_field]):
				doc[loc_field] = str(doc[loc_field][lat_field]) + "," + str(doc[loc_field][lon_field])
				db.save(doc)
				count = count + 1
				if (count % 100 == 0):
					print count

	

def multi_servers():
	for loc in couchdb_locs:
		print loc
		print '----------------'
		change_location(loc,database_name)

multi_servers()
