from mrjob.job import MRJob
from mrjob.step import MRStep

class PatientByCityBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_addressCity,
                   reducer=self.reducer_count_addressCity)
        ]

    def mapper_get_addressCity(self, _, line):
        (pateintId, name, ssn, age, addressNo, addressStreet, addressCity,addressCode, isVaccinated) = line.split(',')
        yield addressCity, 1

    def reducer_count_addressCity(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    PatientByCityBreakdown.run()
