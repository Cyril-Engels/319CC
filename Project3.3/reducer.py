#!/usr/bin/env python

import sys

current_userid=None
output=None
# input comes from standard input
for line in sys.stdin:
    line=line.strip()
    userid,followed=line.split(',')
    if userid==current_userid:
        output+=(':'+followed)
    else:
        if output:
            print output
        current_userid=userid
        output = userid+'\t'+followed
if output:
    print output,