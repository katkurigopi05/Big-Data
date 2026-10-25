from mrjob.job import MRJob
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

class salarymax(MRJob):

    def mapper(self, _, line):
        # Convert each line into a dictionary
        parsed = next(csv.reader([line]), [])
        if not parsed:
            return
        row = dict(zip(cols, [ a.strip() for a in parsed ]))
        if row.get('AnnualSalary') in (None, '', 'AnnualSalary'):
            return

        # Yield the salary
        try:
            yield 'salary', (float(row['AnnualSalary'][1:]), line)
        except ValueError:
            pass
        
        # Yield the gross pay
        try:
            yield 'gross', (float(row['GrossPay'][1:]), line)
        except (ValueError, KeyError):
            self.increment_counter('warn', 'missing gross', 1)

    def reducer(self, key, values):
        topten = []

        # For 'salary' and 'gross' compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]

        for p in topten:
            yield key, p

    combiner = reducer

if __name__ == '__main__':
    salarymax.run()
