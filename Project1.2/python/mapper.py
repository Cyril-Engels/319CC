#!/usr/bin/env python
import sys
import re
import os
# input comes from standard input
filename="pagecounts-20151201-000000"
date=filename.split('-')[1]
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    if len(words) == 4:    
        if re.match('^en$',words[0]) and not re.match('^(MediaWiki_talk|MediaWiki|Media|Special|Talk|User_talk|User|Project_talk|Project|File_talk|File|Template_talk|Template|Help_talk|Help|Category_talk|Category|Portal|Wikipedia_talk|Wikipedia):',words[1]) and re.match(r'^[^a-z]',words[1]) and not re.match(r'.*\.(jpg|gif|png|JPG|GIF|PNG|txt|ico)$|^(404_error\/|Main_Page|Hypertext_Transfer_Protocol|Search)$',words[1]):
    	    print '%s\t%s\t%s' % (words[1], date, words[2])
