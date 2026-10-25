#!/usr/bin/env python
from __future__ import print_function
import re
import sys

for line in sys.stdin:
  val = line.strip()
  if len(val) < 93:
    continue
  temp_str = val[88:92] if val[87] == '+' else val[87:92]
  (year, temp, q) = (val[15:19], int(temp_str), val[92:93])
  if temp == 9999 or temp == -9999:
    sys.stderr.write("reporter:counter:Temperature,Missing,1\n")
  elif re.match("[01459]", q):
    print("%s\t%s" % (year, temp))
