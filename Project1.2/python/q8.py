#!/usr/bin/env python Q7
import sys
output = open('output')
date = 0
views = 0
for line in output:
	line = line.strip()
	tokens = line.split('\t')
	if tokens[1] == 'Interstellar_(film)':
		date = tokens[2].split(':')[0]
		views = int(tokens[2].split(':')[1])
		for i in range(0,29):
			if views < int(tokens[i+3].split(':')[1]):
				views = int(tokens[i+3].split(':')[1])
				date = tokens[i+3].split(':')[0]
output.close()
print date

q5 = open('q5')