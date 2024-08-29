'''CAUTION : This is done from scratch so, parallelization feature of pylucene is not utilised here
program will take almost 4 hours to run. So, I have also provided the output files in submission.'''

# Essential imports:
import pandas as pd
import lucene
import os
import re
import sys
import math
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import BytesRef, BytesRefIterator
from org.apache.lucene.index import DirectoryReader, Term, TermsEnum
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.store import FSDirectory
import org.apache.lucene.document as document
from java.nio.file import Paths
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser

from java.io import File

lucene.initVM()

# Taking Input
if len(sys.argv) != 3:
    print("python3 mtc23XX-searcher.py  /path/to/trec-index-collection/ /path/to/directory/where/query/files/are/stored")
    sys.exit(1)

index = sys.argv[1]
query_file = sys.argv[2]

indexPath = File(index).toPath()
indexPath

index_directory = FSDirectory.open(Paths.get(index))
reader = DirectoryReader.open(index_directory)

field = "TEXT"

numDoc = reader.numDocs()
print(numDoc)
num_terms = 0

# Finding average document length
for i in range(numDoc):
    term_vector = reader.getTermVector(i,field)
    num_terms += term_vector.getSumTotalTermFreq()
ADL = num_terms / numDoc
# print(ADL)

analyzer = StandardAnalyzer()
directory = FSDirectory.open(indexPath)
searcher = IndexSearcher(DirectoryReader.open(directory))

# Scoring documents
def scored_docs(query,numDoc):

    terms = query.split()
    query_length = len(terms)
    w = 2 / (1 + math.log2(1 + query_length))
    
    field = "TEXT"
        
    document_dict = {}
    query = QueryParser(field, analyzer).parse(query)
    
    scoreDocs = searcher.search(query, numDoc).scoreDocs
    
    for i in range(len(scoreDocs)):

        doc_id = scoreDocs[i].doc
        doc = searcher.doc(scoreDocs[i].doc)
        
        term_vector = reader.getTermVector(doc_id,field)
        termsEnumvar = term_vector.iterator()
        termsref = BytesRefIterator.cast_(termsEnumvar)
        term_dict = {}
        
        while (termsref.next()):
            
            termval = TermsEnum.cast_(termsref)
            term_doc = termval.term().utf8ToString()
            tf_doc = termsEnumvar.totalTermFreq() 
            term_dict[term_doc] = tf_doc
            
        # print(term_dict)   
        len_D = sum(term_dict.values())
        num_terms = len(term_dict)
        average_tf = len_D / num_terms

        sum_TDF = 0
        sim = 0

        for term in terms :
            if term in term_dict:
                tf = term_dict[term]
            else:
                tf = 0
            RITF = math.log2(1+tf) / math.log2(1+average_tf) 
            LRTF = tf * math.log2(1 + (ADL/len_D))
            BRITF = RITF / (1 + RITF)
            BLRTF = LRTF / (1 + LRTF)
            
            TFF = (w * BRITF) + ((1 - w) * BLRTF)
        
            docfreq = reader.docFreq(Term('TEXT',term))
            numDoc = reader.numDocs()

            if docfreq != 0:
                IDF = math.log2((numDoc+1)/docfreq)
                CTF = reader.totalTermFreq(Term("TEXT", term))
                AEF =  CTF/ docfreq
                TDF = IDF * (AEF / (1 + AEF))
            else :
                TDF = 0
            sum_TDF += TDF
            sim += TFF * TDF
    
        document_dict[doc.get("DOC_NO")] = sim / sum_TDF
        term_dict.clear()
    return document_dict

# Writing results in file
def write_file(document_dict,query_no):
    if query_no <= 450 :
        with open("trec678.txt", "a") as file:
            i = 1
            for key, value in document_dict.items():
                file.write("%d\t Q0\t %s\t %d\t %f\t CS2320\n"%(query_no, key, i, value))
                i += 1
                if (i>1000):
                    break
    else :
        with open("robust.txt", "a") as file:
            i = 1
            for key, value in document_dict.items():
                file.write("%d\t Q0\t %s\t %d\t %f\t CS2320\n"%(query_no, key, i, value))
                i += 1
                if (i>1000):
                    break

# Processing each query
stop_words = {'a','about','above','after','again','against','ain','all','am','an','and','any','are','aren',"aren't",'as','at','be','because','been','before','being','below','between','both','but','by','can','couldn',"couldn't",'d','did','didn',"didn't",'do','does','doesn',"doesn't",'doing','don',"don't",'down','during','each','few','for','from','further','had','hadn',"hadn't",'has','hasn',"hasn't",'have','haven',"haven't",'having','he','her','here','hers','herself','him','himself','his','how','i','if','in','into','is','isn',"isn't",'it',"it's",'its','itself','just','ll','m','ma','me','mightn',"mightn't",'more','most','mustn',"mustn't",'my','myself','needn',"needn't",'no','nor','not','now','o','of','off','on','once','only','or','other','our','ours','ourselves','out','over','own','re','s','same','shan',"shan't",'she',"she's",'should',"should've",'shouldn',"shouldn't",'so','some','such','t','than','that',"that'll",'the','their','theirs','them','themselves','then','there','these','they','this','those','through','to','too','under','until','up','ve','very','was','wasn',"wasn't",'we','were','weren',"weren't",'what','when','where','which','while','who','whom','why','will','with','won',"won't",'wouldn',"wouldn't",'y','you',"you'd","you'll","you're","you've",'your','yours','yourself','yourselves'}
def query_parser(file_path):

    i = 1
    list_query = []
    
    directory = os.fsencode(file_path)
    
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        f = open(file_path+filename,encoding="latin")
        line = f.readline()
        
        # print("Processing file %dth file -"%i,filename)
        i += 1
        while line :
            if "<num>" in line :
                num = re.sub('<[/]*\w+>', '', line).strip()
            if "<title>" in line :
                query = re.sub('<[/]*\w+>', '', line).lower().strip()
                input_string = query.lower()
                special_chars_pattern = re.compile(r'[^a-zA-Z0-9\s]')
                query = re.sub(special_chars_pattern, ' ', input_string).split()
                filtered_sentence = [word for word in query if word.lower() not in stop_words]
                query = ' '.join(filtered_sentence)
                list_query.append((num,query))
            line = f.readline()
    return list_query

list_query = query_parser(query_file)
len(list_query)

# For each query finding top ranked documents
for query in list_query:
    document_dict = scored_docs(query[1],numDoc)
    sorted_document_dict = dict(sorted(document_dict.items(), key=lambda item: item[1],reverse=True))
    write_file(sorted_document_dict,int(query[0]))
    # print("Processing query %d"%int(query[0]))