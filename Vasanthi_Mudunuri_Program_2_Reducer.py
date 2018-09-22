#!/usr/bin/env python
import sys
import math

movieID=None    #variable movieID
rating=0        #variable rating
count=0         #variable count
sumofratings=0  #variable sumofratings
mean=0.0        #variable mean
ratings=[]      #ratings list
sumofsquares=0.0  #variable sumofsquares
squareofdiff=0.0  #varaible squareofdiff
standarddeviation=0.0  #variable standarddeviation
for line in sys.stdin:  #reading each line from standard input
    line=line.strip()   #removing white spaces if any
    readmovieID,readrating=line.split('\t')  #splitting input and storing in readmovieID and readrating
    rating=int(readrating)   #changing string to int and storing in rating
    if not movieID:          #if movieID is None
        movieID=readmovieID   #for first line, movieID=readmovieID
    if movieID!=readmovieID:   #condition to calculate standarddeviation 
        if count > 0:          #checking count before division
            mean=float(sumofratings)/count  #calculating mean
            for r in ratings:        #iterate through ratings
                squareofdiff=float((r-mean)*(r-mean)) #calculate squareofdiff
                sumofsquares+=squareofdiff   #calculating sumofsquares
            standarddeviation=math.sqrt(float(sumofsquares)/count) #calculating standarddeviation
            result=[movieID,standarddeviation]  #storing movieID and standarddeviation in result list
        print("\t".join(str(v) for v in result)) #printing movieID and standarddeviation
        movieID=readmovieID   #reinitialising all variables for calculating next movie standarddeviation
        count=0
        rating=0
        sumofratings=0
        sumofsquares=0.0
        squareofdiff=0.0
        mean=0.0
        ratings=[]
        standarddeviation=0.0
    else:
        ratings.append(rating) #appending all ratings in ratings list
        sumofratings+=rating   #calculating sumofratings
        count+=1               #calculating noofratings
