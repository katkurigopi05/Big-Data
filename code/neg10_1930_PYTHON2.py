#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

GOOD_QUALITY = set("01459")  # allowed quality flags

class Neg10In1930(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_filter, reducer=self.reducer_aggregate)]

    def mapper_filter(self, _, line):
        # Need enough chars to parse fixed-width fields
        if not line or len(line) < 93:
            return

        # Year from date field YYYYMMDD starting at col 16 (0-based 15)
        year = line[15:19]

        # Air temperature (tenths of °C), sign may be present
        if line[87] in "+-":
            temp_str = line[87:92]
        else:
            temp_str = line[88:92]

        # Guard against non-numeric/missing values
        try:
            air_temp = int(temp_str)
        except ValueError:
            return

        # Skip missing sentinel if present
        if air_temp == 9999:
            return

        quality = line[92]

        # Match: 1930 and exactly -10.0°C (i.e., -100 in tenths), good quality
        if year == "1930" and air_temp == -100 and quality in GOOD_QUALITY:
            station_usaf = line[0:6]
            station_wban = line[7:12]
            date = line[15:23]  # YYYYMMDD
            match_detail = "{}-{}\t{}\t-10.0C".format(station_usaf, station_wban, date)

            # Emit a listing record and a counter record
            yield (("MATCH", match_detail), 1)
            yield (("COUNT", "TOTAL"), 1)

    def reducer_aggregate(self, key, values):
        group, detail = key
        if group == "MATCH":
            # pass-through each match (one line per matching record)
            for _ in values:
                yield ("MATCH", detail), 1
        else:
            # sum total count
            yield ("COUNT", "TOTAL"), sum(values)

if __name__ == "__main__":
    Neg10In1930.run()