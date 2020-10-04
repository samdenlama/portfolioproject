import psycopg2
import csv
try:

	with open('covid-19_diagnostic_lab_testing_20200919_2049.csv') as csv_file:
		
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
				postgres_insert_query = """ INSERT INTO COVID_DATA 
				(state,state_name,state_fips,fema_region,overall_outcome,test_date,new_results_reported,total_results_reported) 
				VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
				record_to_insert = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7])
				cursor.execute(postgres_insert_query, record_to_insert)
				connection.commit()
			line_count += 1		
		
		print(f'{line_count} records added to table COVID_DATA.')
		
except (Exception, psycopg2.Error) as error :
    print ("Error while inserting to database", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("Database connection is closed.")