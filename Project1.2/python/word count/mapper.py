#!/usr/bin/env python

import sys

#input comes fron STDIN (standard input)
for line in sys.stdin:
	#remove leading and trailing whitespace
	line = line.strip()
	#split the line into words
	words = line.split()
	#increase counters
	for word in words:
		#write the results to STDOUT (standard outout)
		#what we output here will be the input for the 
		#Reduce step, i.e. the input for reduce.py 
		#
		#tab-delimited; the trivial word count is 1 
		print '%s\t%s' % (word,1)
