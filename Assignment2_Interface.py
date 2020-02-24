
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cur = openconnection.cursor()
    rangeprefix = 'RangeRatingsPart'
    cur.execute('Select * from RangeRatingsMetadata;')
    metarows = cur.fetchall()
    tables_to_check = list()
    for row in metarows:
        left_limit = row[1]
        right_limit = row[2]
        partition_number = row[0]
        if left_limit > ratingMaxValue or right_limit < ratingMinValue:
            continue
        else:
            tables_to_check.append(rangeprefix+str(partition_number))
    # print(tables_to_check)
    subqueries = ['Select \'' + this_partition + '\',* from ' + this_partition +
                  ' where ' + this_partition+'.rating between ' + str(ratingMinValue) + ' and ' +
                  str(ratingMaxValue) + '' for this_partition in tables_to_check]
    # print(subqueries)
    query = ' union '.join(subqueries)
    cur.execute(query)
    res = cur.fetchall()
    with open(outputPath, 'a') as file:
        for r in res:
            file.write(str(r[0]) + ',' + str(r[1]) + ',' + str(r[2]) + ',' + str(r[3]) + '\n')

    robinprefix = 'RoundRobinRatingsPart'
    cur.execute('Select partitionnum from RoundRobinRatingsMetadata;')
    numPartitions = cur.fetchall()[0][0]
    for i in range(numPartitions):
        tableName = robinprefix + str(i)
        cur.execute('Select * from ' + tableName +
                    ' where rating >= ' + str(ratingMinValue) + ' and rating <= ' + str(ratingMaxValue) + ';')
        res = cur.fetchall()
        with open(outputPath, 'a') as file:
            for r in res:
                file.write(str(tableName) + ',' + str(r[0]) + ',' + str(r[1]) + ',' + str(r[2]) + '\n')


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    cur = openconnection.cursor()
    rangeprefix = 'RangeRatingsPart'
    cur.execute('Select * from RangeRatingsMetadata;')
    metarows = cur.fetchall()
    tables_to_check = list()
    for row in metarows:
        left_limit = row[1]
        right_limit = row[2]
        partition_number = row[0]
        if left_limit > ratingValue or right_limit < ratingValue:
            continue
        else:
            tables_to_check.append(rangeprefix+str(partition_number))
    # print(tables_to_check)
    subqueries = ['Select \'' + this_partition + '\',* from ' + this_partition +
                  ' where ' + this_partition+'.rating = ' + str(ratingValue) + '' for this_partition in tables_to_check]
    query = ' union '.join(subqueries)
    cur.execute(query)
    res = cur.fetchall()
    with open(outputPath, 'a') as file:
        for r in res:
            file.write(str(r[0]) + ',' + str(r[1]) + ',' + str(r[2]) + ',' + str(r[3]) + '\n')

    robinprefix = 'RoundRobinRatingsPart'
    cur.execute('Select partitionnum from RoundRobinRatingsMetadata;')
    numPartitions = cur.fetchall()[0][0]
    for i in range(numPartitions):
        tableName = robinprefix + str(i)
        cur.execute('Select * from ' + tableName +
                    ' where rating = ' + str(ratingValue) + ';')
        res = cur.fetchall()
        with open(outputPath, 'a') as file:
            for r in res:
                file.write(str(tableName) + ',' + str(r[0]) + ',' + str(r[1]) + ',' + str(r[2]) + '\n')
