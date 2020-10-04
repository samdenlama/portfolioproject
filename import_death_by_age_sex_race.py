import psycopg2
import csv
try:

	with open('Cumulative_Provisional_Counts_of_Deaths_by_Sex__Race__and_Age.csv') as csv_file:
		
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
				postgres_insert_query = """ INSERT INTO DEATH_COUNTS 
				(date_as_of,sex,race,agegroup,number_of_covid19_deaths) 
				VALUES (%s,%s,%s,%s,%s)"""
				record_to_insert = (row[0],row[1],row[2],row[3],row[22])
				cursor.execute(postgres_insert_query, record_to_insert)
				connection.commit()
			line_count += 1		
		
		print(f'{line_count} records added to table DEATH_COUNTS.')
		
except (Exception, psycopg2.Error) as error :
    print ("Error while inserting to database", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("Database connection is closed.")