import psycopg2
import csv
try:

	with open('Conditions_contributing_to_deaths_involving_coronavirus_disease_2019__COVID-19___by_age_group_and_state__United_States.csv') as csv_file:
		
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
				if row[0] in (None,""):
					postgres_insert_query = """ INSERT INTO CONDITION_CONTRIBUTING_DEATHS 
					(date_as_of,start_week,end_week,state,condition_group,condition,age_group,number_of_covid19_deaths) 
					VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
					record_to_insert = (row[0],row[1],row[2],row[3],row[4],row[5],row[7],row[8])
					cursor.execute(postgres_insert_query, record_to_insert)
					connection.commit()
			line_count += 1		
		
		print(f'{line_count} records added to table CONDITION_CONTRIBUTING_DEATHS.')
		
except (Exception, psycopg2.Error) as error :
    print ("Error while inserting to database", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("Database connection is closed.")