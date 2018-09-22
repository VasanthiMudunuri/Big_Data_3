#!/usr/bin/env python
import sys

movieID=""
movieid=""
rating=0
movieMap = {'key': 'value'}
with open('movies.dat','r') as readfile: #opening movies.dat file to read
    for line in readfile:        #reading each line
        record = line.split('::') #splitting by '::' as seperator
        movieID = record[0]
        movieName = record[1]
        movieMap[movieID] = movieName #storing movieID and movieName as key value in movieMap
for line in sys.stdin:      #reading each line from standard input
    record = line.split('::')  #splitting by '::' as seperator
    movieid = record[1]
    rating = record[2]
    compositekey=movieMap[movieid]+"."+rating   #creating composite key by '.' as seperator
    print '%s.%s' %(compositekey,"") #printing compositekey as output
