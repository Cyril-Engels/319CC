#!/usr/bin/env python Q7
import sys
cities = {}
city_list=''
q7 = open('q7')
for line in q7.readlines():
	line = line.strip()
	cities[line] = 0
q7.close()
cities2 = sorted(cities.iteritems(),key = lambda d:d[0],reverse = False)

output = open('output')
for line in output:
	line = line.strip()
	tokens=line.split('\t')
	for i in range(0,len(cities2)):
		if cities2[i][0] == tokens[1]:
			cities[tokens[1]] = int(tokens[0])
output.close()
cities = sorted(cities.iteritems(),key = lambda d:d[1],reverse = True)
for i in range(0,len(cities)-1):
	city_list += (cities[i][0] + ',')
city_list += cities[len(cities)-1][0]
print city_list