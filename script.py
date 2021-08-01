from os import device_encoding
import mysql.connector


my_file = open("data.txt")
content = my_file.read()
#print(content)
content_list = content.split("|D|")
my_file.close()
heading = content_list[0]
content_list.remove(heading)
data = content_list

insert_cols = heading.split('|')
insert_cols.pop(0)
insert_cols.pop(0)
heading = heading.split('|')
heading.pop(0)
heading.pop(0)

heading.insert(0, 'id SERIAL PRIMARY KEY NOT NULL')
heading[1] += ' VARCHAR(255) NOT NULL'
heading[2] += ' VARCHAR(18) NOT NULL'
heading[3] += ' DATE NOT NULL'
heading[4] += ' DATE'
heading[5] += ' CHAR(5)'
heading[6] += ' CHAR(255)'
heading[7] += ' CHAR(5)'
heading[8] += ' CHAR(5)'
#heading[8] += ' INT(5)'
heading[9] += ' DATE'
heading[10] += ' CHAR(1)'

#print(insert_cols)

columns_to_create = ','.join(heading)

#print(columns_to_create)

cnx = mysql.connector.connect(host="localhost", user='root', password='admin1234', database='practice')
cursor = cnx.cursor()
# print(cursor)

#print(data)
table_deletion_queries = []
table_creation_queries = {}
table_insertion_queries = []
table_insertion_values = []
for  d in data:
    d_array = d.split('|')
    table_deletion_queries.append('drop table if exists table_'+d_array[7]+';')
    table_creation_queries[d_array[7]] = 'create table if not exists table_'+ d_array[7] +' (' + columns_to_create + ')'
    
    d_array[8] = d_array[8][4:] + d_array[8][2:4] + d_array[8][:2]
    # print(d_array[8])
    table_insertion_queries.append('insert into table_'+d_array[7] + '(' + ','.join(insert_cols) + ') values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'); 
    table_insertion_values.append(tuple(d_array))
    #print(tuple(d_array))
#print(table_creation_queries)

#print(table_insertion_queries)

for deletion_query in table_deletion_queries:
   # print(deletion_query)
    cursor.execute(deletion_query)

for key, creation_query in table_creation_queries.items():
    # print(key)
    cursor.execute(creation_query)
    
for index, insertion_query in enumerate(table_insertion_queries):
    #print('inserted')
    #cursor.execute('insert into table_au(Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active) values ("Jacob","1256","20101012","20121013","MVD","Paul","VIC","AU","19870306","A")')
    cursor.execute(insertion_query, table_insertion_values[index])
    #print(insertion_query)
    #print(table_insertion_values[index])
    cnx.commit()

