from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
import re
r='[!"#$%&\'()*+,-./:;<=>?@[\\]^_{|}~]+'
class WordCount(MRJob):
		INPUT_PROTOCOL = JSONValueProtocol
		def mapper(self, _, data):
			if data['type']=='review':
				tmp = data['text']
				#tmp = rmp.replace("~","-")
				tmp = tmp.strip()
				words = tmp.split()
				for word in words:
					word = re.sub(r,'',word)
					word = word.lower()
					if(word != 'this' and word != 'that' and word != 'the' and word != 'a' and word != '.' and word != 'some'):
						yield (word,1)


		def combiner(self,year,counts):
			yield(year,sum(counts))

		def reducer(self,year,counts):
			yield(year,sum(counts))	



if __name__ == '__main__':
	WordCount.run()