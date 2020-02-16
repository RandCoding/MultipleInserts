# Testing connection to azure sql server
import pyodbc

# server = 'server.database.windows.net' #server link 
# db = 'dbName'
# usern = 'YourUserId'
# pwd = 'Pwd'
con = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server=tcp:server.database.windows.net,1433;Database=airlines;Uid=YourUser;Pwd={YourPwd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

cursor = con.cursor()
for row in cursor.tables():
    print(row.table_name)