#!/usr/bin/env python Q5
import sys

First = [0 for i in range(0, 30)]
Second = [0 for i in range(0, 30)]
list = ['' , '']

q5 = open('q5')
i = 0
for line in q5:
	list[i] = line.strip()
	i += 1
first = list[0]
second = list[1]

input_file = open('output')
for line in input_file:
	line = line.strip()
	words = line.split('\t') 
	if words[1] == first:
		for i in range(30):			
			First[i]=int(words[i+2].split(':')[1])
	elif words[1] == second:
		for i in range(30):
			Second[i]=int(words[i+2].split(':')[1])
j=0
for i in range(30):
	if First[i] - Second[i] > 0:
		j += 1
print j
