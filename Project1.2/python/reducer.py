#!/usr/bin/env python

import sys

current_title = None
total_count = 0
daily_count = [0 for i in range(31)]

# input comes from standard input
for line in sys.stdin:
    title, date, count = line.split('\t', 2)   
    date = int(date)

    try:
        count = int(count)
    except ValueError:
        continue

    if current_title == title:
    	daily_count[date-20151201] += count
        total_count += count

    else:
        if current_title:
        	if total_count > 100000:
        		output = str(total_count) + '\t' + current_title + '\t'
        		for i in range(31):
        			output += (str(20151201+i)+':'+str(daily_count[i])+'\t')
        		print output
                daily_count = [0 for i in range(31)]
        total_count = count
        daily_count[date-20151201]=count
        current_title = title
if current_title == title and total_count > 100000:
	output = str(total_count) + '\t' + current_title + '\t'
	for i in range(31):
		output += (str(20151201+i)+':'+str(daily_count[i])+'\t')
	print output
daily_count = [0 for i in range(31)]
