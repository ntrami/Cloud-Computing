# Copyright 2009-2012 Yelp
# Copyright 2017 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for JobConf Environment Variables
"""
import re

from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob

WORD_RE = re.compile(r"[\w']+")


# class MRWordCount(MRJob):
#     """ Trivial Job that returns the number of words in each input file
#     """
#     def mapper(self, _, line):
#         for word in WORD_RE.findall(line):
#             yield (jobconf_from_env("mapreduce.map.input.file"), 1)

#     def combiner(self, name, counts):
#         self.increment_counter('count', 'combiners', 1)
#         yield name, sum(counts)

#     def reducer(self, name, counts):
#         yield (name, sum(counts))


# if __name__ == '__main__':
#     MRWordCount.run()


from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
    # MRWordCount.run()
