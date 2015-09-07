#-----------------------------------------------------DDS ASSIGN 3-------------------------------------------------------------------#

import psycopg2
import thread
import threading
from time import sleep
from random import randint




#--------------------------------------------------------------------------------------------------------------------------------------#
# Get Connection
def getopenconnection(user='postgres', password='root', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


#--------------------------------------------------------------------------------------------------------------------------------------#
# Range Partioning Algorithm
def rangepartition(tablename, numberofpartitions, minimum, maximum, attr, openconnection):

    # create a cursor
    cur = openconnection.cursor()
    # create the MetaData table
    cur.execute("CREATE TABLE "+tablename+"meta(Low REAL, High REAL, TableName TEXT)")
    #caluculating the band for partioning
    N = numberofpartitions
    band = N/(maximum-minimum)
    # create the fragments and insert the relevent metadata into the RangeMetaData table
    for i in range(0, N):
        cur.execute("CREATE TABLE "+tablename+"part" + str(i) + " AS SELECT * FROM "+tablename+" WHERE FALSE;")
        # NOTE: the 'high' of one range and the 'low' of the next range will be the same 
        # number in the metadata table thus the rnage is inclusive of the 'high' but not the 'low'
        low = '0' if i == 0 else str(i*band)
        high = '5' if i == N-1 else str((i+1)*band)
        cur.execute("INSERT INTO "+tablename+"meta"+" VALUES('" + low + "','" + high + "','"+tablename+"part" + str(i) + "')")

    # Populate the partitioned tables according to the Ratings value
    openconnection.commit()
    cur.execute("select column_name from information_schema.columns where table_name = '"+str(tablename)+"'")
    names = cur.fetchall()
    a_index=names.index((attr,))
    cur.execute("SELECT * FROM "+tablename)
    rows = cur.fetchall()
    
    for row in rows:
        cur.execute("SELECT TableName FROM "+tablename+"meta WHERE Low<" + str(row[a_index]) + " AND High>=" + str(row[a_index]))
        name = cur.fetchone()[0]
        cur.execute("INSERT INTO " + name + " VALUES" + str(row))

    openconnection.commit()
    cur.close()
    pass


#--------------------------------------------------------------------------------------------------------------------------------------#
# Thread function for Parallel Sort
def parSortThread(tablename,partition,col,openconnection):
    cur = openconnection.cursor()
    new_partition = "Sorted_"+tablename+str(partition-1)
    cur.execute("CREATE TABLE "+new_partition+" AS SELECT * FROM "+tablename+"part"+str(partition-1)+" ORDER BY "+col+";")
    openconnection.commit()
    cur.execute("DROP TABLE "+tablename+"part"+str(partition-1)+";")
    
    pass


#--------------------------------------------------------------------------------------------------------------------------------------#
# Parallel Sort Method
def ParallelSort(Table, SortingColumnName, OutputTable, openconnection):
    
    # Create a Cursor
    cur = openconnection.cursor()

    # Get minimum and maximum in both the tables
    cur.execute("SELECT MIN("+SortingColumnName+") FROM "+Table+";")
    minimum = int(cur.fetchone()[0])
    cur.execute("SELECT MAX("+SortingColumnName+") FROM "+Table+";")
    maximum = int(cur.fetchone()[0])

    # Create 5 partitions based on range 
    rangepartition(Table, 5, minimum, maximum, SortingColumnName, openconnection)

    #Create threads equal to the number of partitions
    cur.execute("SELECT COUNT(*) FROM "+Table+"meta;")
    threadCount = int(cur.fetchone()[0])
    threadList = []
    for t in range(0,threadCount):
        thr = threading.Thread(target=parSortThread, args=(Table,t+1,"rating",openconnection))
        threadList.append(thr)
    for tr in threadList:
        tr.start()
    for thread in threadList:
        thread.join()

    # Create a table to store the output
    cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM "+Table+" WHERE FALSE;")
    cur.execute("ALTER TABLE "+OutputTable+" ADD tupleorder INT;") 
    # Insert into the output table
    i=0
    for t in range(0,threadCount):
        cur.execute("SELECT * FROM SORTED_"+Table+str(t)+";")
        data=cur.fetchall();
        for tup in data:
            i=i+1
            tup=tup+(i,)
            cur.execute("INSERT INTO "+OutputTable+" VALUES"+str(tup)+";")
        con.commit()
        cur.execute("DROP TABLE SORTED_"+Table+str(t)+";");                        

    openconnection.commit()
    cur.execute("DROP TABLE "+Table+"meta;")
    openconnection.commit()
    pass


#--------------------------------------------------------------------------------------------------------------------------------------#
# Thead function for JOIN
    
def parJoinThread(tablename1,tablename2,partition,col1,col2,openconnection):
    cur = openconnection.cursor()
    new_partition = "Join_"+str(partition-1)
    cur.execute("CREATE TABLE "+new_partition+" AS SELECT * FROM "+tablename1+"part"+str(partition-1)+" JOIN "+tablename2+"part"+str(partition-1)+" ON("+tablename1+"part"+str(partition-1)+"."+col1+"="+tablename2+"part"+str(partition-1)+"."+col2+");")
    openconnection.commit()
    cur.execute("DROP TABLE "+tablename1+"part"+str(partition-1)+";")
    cur.execute("DROP TABLE "+tablename2+"part"+str(partition-1)+";")
    pass


#--------------------------------------------------------------------------------------------------------------------------------------#
# Parallel Join

def ParallelJoin(Table1, Table2,Table1JoinColumn, Table2JoinColumn,OutputTable,openconnection):

    #Create a cursor
    cur = openconnection.cursor()

    #getting the min and max for the range partitioning
    cur.execute("SELECT MIN("+Table1JoinColumn+") FROM "+Table1+";")
    minimum = int(cur.fetchone()[0])
    cur.execute("SELECT MAX("+Table1JoinColumn+") FROM "+Table1+";")
    maximum = int(cur.fetchone()[0])
    rangepartition(Table1, 5, minimum, maximum, Table1JoinColumn, getopenconnection())
    openconnection.commit()
    rangepartition(Table2, 5, minimum, maximum, Table2JoinColumn, getopenconnection())
    openconnection.commit()
    cur.execute("SELECT COUNT(*) FROM "+Table1+"meta;")
    threadCount = int(cur.fetchone()[0])
    
    threadList = []
    for t in range(0,threadCount):
        thr = threading.Thread(target=parJoinThread, args=(Table1,Table2,t+1,"rating","rat",openconnection))
        threadList.append(thr)
    for tr in threadList:
        tr.start()
    for thread in threadList:
        thread.join()

    cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM JOIN_0 WHERE FALSE;")
    for t in range(0,threadCount):
        cur.execute("INSERT INTO "+OutputTable+" SELECT * FROM JOIN_"+str(t)+";")
        openconnection.commit()
        cur.execute("DROP TABLE JOIN_"+str(t)+";")
                
    openconnection.commit()
    cur.execute("DROP TABLE "+Table1+"meta;")
    cur.execute("DROP TABLE "+Table2+"meta;")
    pass


#--------------------------------------------------------------------------------------------------------------------------------------#
#Main Function

if __name__ == '__main__':
    
    #ParallelSort('ratings','rating','sortedResult',getopenconnection())
    #ParallelJoin('ratings','movies','rating','rat','JoinOutput',getopenconnection())
