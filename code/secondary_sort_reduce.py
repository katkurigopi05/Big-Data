#!/usr/bin/env python
from __future__ import print_function
import sys

last_group = None
for line in sys.stdin:
  val = line.strip()
  if not val:
    continue
  (year, temp) = val.split("\t")
  group = year
  if last_group != group:
    print(val)
    last_group = group
