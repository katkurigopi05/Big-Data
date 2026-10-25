#!/usr/bin/env python
from __future__ import print_function
import sys

(last_key, max_val) = (None, -sys.maxsize)
for line in sys.stdin:
    val = line.strip()
    if not val:
        continue
    (key, val) = val.split("\t")
    if last_key and last_key != key:
        print("%s\t%s" % (last_key, max_val))
        (last_key, max_val) = (key, int(val))
    else:
        (last_key, max_val) = (key, max(max_val, int(val)))

if last_key:
    print("%s\t%s" % (last_key, max_val))
