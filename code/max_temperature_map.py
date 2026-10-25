#!/usr/bin/env python
from __future__ import print_function
import re
import sys

for line in sys.stdin:
    val = line.strip()
    if len(val) < 93:
        continue
    (year, temp, q) = (val[15:19], val[87:92], val[92:93])
    if temp != "+9999" and temp != "-9999" and re.match("[01459]", q):
        print("%s\t%s" % (year, temp))
