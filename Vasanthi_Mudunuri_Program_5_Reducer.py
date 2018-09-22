#!/usr/bin/env python
import sys

movieTitle = None
ratings=[]
moviename = None
for line in sys.stdin:   #reading each line from standard input
    record = line.split('.') #splitting by '.' as seperator
    movieTitle = record[0]
    if moviename != movieTitle:   #condition to print movietitle and minimum ratings
        if moviename:
            print '%s\t%s' %(movieTitle,min(ratings)) #printing output
        moviename = movieTitle
        ratings=[]    #emptying list for next calculation
        ratings.append(record[1]) #appending all ratings
    else:
        ratings.append(record[1]) #appending all ratings
