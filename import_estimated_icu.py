import psycopg2
import csv
try:

	with open('estimated_icu_20200928_1129.csv') as csv_file:
		
		connection = psycopg2.connect(user = "postgres",
									  password = "",
									  host = "127.0.0.1",
									  port = "5432",
									  database = "DMAS")

		cursor = connection.cursor()
	
		csv_reader = csv.reader(csv_file, delimiter=',')
		line_count = 0
		for row in csv_reader:
			if line_count > 0:
				postgres_insert_query = """ INSERT INTO ICU_BEDS_OCCUPANCY 
				(state,collection_date,estimated_beds_occupied,total_icu_beds) 
				VALUES (%s,%s,%s,%s)"""
				record_to_insert = (row[0],row[1],row[2].replace(',',''),row[8].replace(',',''))
				cursor.execute(postgres_insert_query, record_to_insert)
				connection.commit()
			line_count += 1		
		
		print(f'{line_count} records added to table ICU_BEDS_OCCUPANCY.')
		
except (Exception, psycopg2.Error) as error :
    print ("Error while inserting to database", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("Database connection is closed.")