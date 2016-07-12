#!/usr/bin/env python

import sys

output=None
# input comes from standard input
for line in sys.stdin:
    line=line.strip()
    userid,date,link=line.split(',')
    output = "url\x03{\"s\":\""+link+"\"}\x02userid\x03{\"n\":\""+userid+"\"}\x02time\x03{\"s\":\""+date+"\"}";
    print output
    output=None

    #url\x03{"s":"https://soundrrr.s3.amazonaws.com/0000210873e35b413b322fb9971275.png"}\x02userid\x03{"n":"1"}\x02time\x03{"s":"2012-11-16"}