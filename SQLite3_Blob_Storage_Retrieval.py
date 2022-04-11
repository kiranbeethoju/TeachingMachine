import datetime
import sqlite3
import pandas as pd
# As blob you can any type of files on sqlite3 table and write those files as files by retrieving them.
# Run only once while creating database

db = sqlite3.connect("FilesDatabase.db")
#db.execute("drop table if exists files_table")
db.execute("""
           create table files_table
           (date datetime,binary_file BLOB,name TEXT)
           """)
           
def writeTofile(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)
    print("Stored blob data into: ", filename, "\n")

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData

# Write blob to file
def readBlobData(file_name):
    try:
        nameOfFile = file_name.split(".")[0]
        extension = file_name.split(".")[1]
        sqliteConnection = sqlite3.connect('FilesDatabase.db')
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")
        sql_fetch_blob_query = "SELECT * from files_table where name = ?;"
        cursor.execute(sql_fetch_blob_query,(nameOfFile,))
        records = cursor.fetchall()
        for row in records:
            print(len(row))
            tableFile = row[1]
            #print(tableFile,row[0])
            print("Storing file to disk \n")
            Savefile_name = "{}.{}".format(nameOfFile,extension)
            writeTofile(tableFile, Savefile_name)
        cursor.close()
        return  
    except sqlite3.Error as error:
        print("Failed to read blob data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("sqlite connection is closed")

# insert records
def insertRecords(file_name):
    timenow = str(datetime.datetime.now()).split(".")[0]
    try:
        sqliteConnection = sqlite3.connect('FilesDatabase.db')
        cursor = sqliteConnection.cursor()
        EncodedData= convertToBinaryData(file_name)
        sqlite_insert_blob_query = """ INSERT INTO files_table (date,binary_file,name) VALUES (?,?,?)"""
        #print(sqlite_insert_blob_query)
        file_name_id = file_name.split(".")[0]
        file_ext = file_name.split(".")[-1]
        data_tuple = (timenow, EncodedData,file_name_id)
        cursor.execute(sqlite_insert_blob_query,data_tuple)
        sqliteConnection.commit()
        print("{} - File inserted successfully as a BLOB into a table".format(file_name))
        cursor.close()
        return "success insertRecords"
    except sqlite3.Error as error:
        print("Failed to insert blob data into sqlite table", error)
        return "Fail insertRecords"
    if sqliteConnection:
        sqliteConnection.close()
        print("the sqlite connection is closed")
#%%
#Update Records
            
def UpdateRecords(file_name):
    timenow = str(datetime.datetime.now()).split(".")[0]
    try:
        file_name_id = file_name.split(".")[0]
        file_ext = file_name.split(".")[-1]
        sqliteConnection = sqlite3.connect('FilesDatabase.db')
        cursor = sqliteConnection.cursor()   
        sqlite_insert_blob_query =  """UPDATE files_table SET date='"""+ str(timenow) +"""' where name='""" +str(file_name_id)+"""';"""
        cursor.execute(sqlite_insert_blob_query)
        sqliteConnection.commit()
        print("updated info")
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to update table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("the sqlite connection is closed")

def PendingFilequery(q):
    sqliteConnection = sqlite3.connect('FilesDatabase.db')
    try:
        res = pd.read_sql("{}".format(q),con=sqliteConnection)
        sqliteConnection.close()
        #res.to_csv("latest.csv")
        return res
    except:
        sqliteConnection.close()
        return "Failed to query the data"

