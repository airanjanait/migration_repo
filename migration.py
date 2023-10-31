import pymysql

source_conn=pymysql.connect(host="localhost",user="root",password="",database="company")
destination_conn=pymysql.connect(host="localhost",user="root",password="",database="bucketlist")


# Create cursors for source and destination databases
source_cursor = source_conn.cursor()
destination_cursor = destination_conn.cursor()

#-----------------creating tables in destination table which is present in source table------------

#___________Get table names from source database
source_cursor.execute("SHOW TABLES")
tables = source_cursor.fetchall()

# Iterate over each table
for t in tables:
    table_name = t[0]
    print(f"Migrating data from table: {table_name}")

#____________Retrieve table schema from source database
    source_cursor.execute(f"DESCRIBE {table_name}")
    source_columns = source_cursor.fetchall()
    print(source_columns)

    
    for d in source_columns:
        column_name=d[0]
  
#__________Create table in destination database with the same schema
    print("______________Creating table in destination database with the same schema______________")
    create_table_query = f"CREATE TABLE {table_name} ("
    for column in source_columns:
        column_name = column[0]
        column_type = column[1]
        create_table_query += f"{column_name} {column_type}, "
    create_table_query = create_table_query[:-2]  # Remove the last comma and space
    create_table_query += ")"
    d=destination_cursor.execute(create_table_query)
    destination_conn.commit()
    print(d)
    print(create_table_query)
    print("your table successfully created in destination database")

#___________Retrieve data from the source table

    print("____________selecting * from source table________________")
    source_cursor.execute(f"SELECT * FROM {table_name}")
    data = source_cursor.fetchall()
    print(data)

#__________Insert data into the destination table
    for row in data:
       
        transformed_row = row  
        
        insert_query = f"INSERT INTO {table_name} VALUES ("
        print(insert_query)
        for value in transformed_row:
             insert_query += f"'{value}', "
        insert_query = insert_query[:-2]  
        insert_query += ")"

        
        destination_cursor.execute(insert_query)
        destination_conn.commit()
        print("successfully inserted")
        

        

