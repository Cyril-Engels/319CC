#!/usr/bin/env python Q7
import sys
output = open('output')
q5 = open('q5')
globalMaximum = 1
currentMaximum = 1
tempt = 1
count = 0

for line in output:
	line = line.strip()
	tokens = line.split('\t')
	for i in range(0,29):
		if int(tokens[i+2].split(':')[1]) < int(tokens[i+3].split(':')[1]):
			currentMaximum += 1
		else:
			if currentMaximum > tempt:
				tempt = currentMaximum
			currentMaximum = 1
	if tempt > globalMaximum:
		globalMaximum = tempt
		count = 1
	elif tempt == globalMaximum:
		count += 1
	currentMaximum = 1
	tempt = 1
output.close()
print count

