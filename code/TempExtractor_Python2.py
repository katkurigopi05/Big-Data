#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# ncdc_year_temp.py
# MRJob to extract (Year, Temperature) pairs from NCDC fixed-width records.
# Output lines: "<year>\t<temp>"

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class YearTempExtractor(MRJob):
    # Write raw lines instead of JSON so downstream Pig/Hive can read TSV cleanly
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        # Defensive parsing for short/blank lines
        if not line or len(line) < 93:
            return

        # YEAR (common NCDC layout)
        year = line[15:19]

        # AIR TEMPERATURE around positions [87:92]
        # Optional leading sign at [87]; missing value typically 9999
        sign_char = line[87]

        if sign_char in ('+', '-'):
            temp_str = line[88:92]
            try:
                temp = int(temp_str)
            except ValueError:
                return
            if sign_char == '-':
                temp = -temp
        else:
            try:
                temp = int(line[87:92])
            except ValueError:
                return

        # Filter out missing sentinel values
        if abs(temp) == 9999:
            return

        # Optional quality filter (often at [92]); uncomment if required
        # quality = line[92]
        # if quality not in ('0', '1', '4', '5', '9'):
        #     return

        # Emit as one raw TSV line
        yield None, "%s\t%d" % (year, temp)


if __name__ == "__main__":
    YearTempExtractor.run()