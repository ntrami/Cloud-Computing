from mrjob.job import MRJob

class MRCountSum(MRJob):

    def mapper(self, _, line):
        line = line.strip() # remove leading and trailing whitespace
        if line.find("From:") == 0 and line.find("@") > 0 and line.find(">") > 0:
            email_domain = line[line.find("@")+1:line.find(">")]
            if len(email_domain) == 0:
                email_domain = "empty"
            else:
                yield email_domain, 1 # The yield statement suspends a function's execution and sends a value back to the caller,
                # but retains enough state to enable the function to resume where it left off.
                # When the function resumes, it continues execution immediately after the last yield run.
                # This allows its code to produce a series of values over time, rather than computing them at once
                # and sending them back like a list.

    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRCountSum.run()
