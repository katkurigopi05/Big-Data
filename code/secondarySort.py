from mrjob.job import MRJob
import re
import sys

class secondary_sort(MRJob):

    def mapper(self, _, line):
        
        val = line.strip()
        (year, temp, q) = (val[15:19], int(val[87:92]), val[92:93])
        if temp == 9999:
          sys.stderr.write("reporter:counter:Temperature,Missing,1\n")
        elif re.match("[01459]", q):
          yield year, (temp, val)

    def reducer(self, key, values):
        
        record = []
        
        for p in values:
            record.append(p)
            
        record.sort(reverse=True)
        yield key, record

if __name__ == '__main__':
    secondary_sort.run()