#!/usr/bin/env python

import sys
for line in sys.stdin: #reading each line from standard input
    movieID=""         #variable movieID
    rating=0           #variable rating
    line=line.strip()  #removing white spaces if any
    splits=line.split('::')   #splitting the line by '::' as seperator
    movieID=splits[1]         #second split stored in movieID
    rating=splits[2]          #third split stored in rating
    print ('%s\t%s' %(movieID,rating)) #print movieID and rating
