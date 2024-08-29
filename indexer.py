# Essential imports:
import pandas as pd
import lucene
import os
import re
import sys
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.store import FSDirectory
import org.apache.lucene.document as document
from org.apache.lucene.document import Field, FieldType

lucene.initVM()

# Taking Input
if len(sys.argv) != 3:
    print("Usage: python3 mtc23XX-indexer.py  /path/to/trec-robust-collection/ /path/to/directory/where/index/should/be/stored")
    sys.exit(1)

collection = sys.argv[1]
index = sys.argv[2]

indexPath = File(index).toPath()
indexDir = FSDirectory.open(indexPath)

writerConfig = IndexWriterConfig(StandardAnalyzer())
writer = IndexWriter(indexDir, writerConfig)

def indexMovie(doc_no, text):
    field_type = FieldType()
    field_type.setStored(True)
    field_type.setTokenized(True)
    field_type.setStoreTermVectors(True)
    field_type.setStoreTermVectorPositions(True)
    field_type.setStoreTermVectorOffsets(True)
    field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
    
    doc = document.Document()
    doc.add(document.Field("DOC_NO", doc_no, document.TextField.TYPE_STORED))
    doc.add(Field("TEXT", text, field_type))
    writer.addDocument(doc)

def closeWriter():
    writer.close()

def makeIndex(file_path):

    # i = 1
    directory = os.fsencode(file_path)
    
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        f = open(file_path+filename,encoding="latin")
        line = f.readline()
        
        # print("Processing file %dth file -"%i,filename)
        # i += 1
        while line :
            if "<DOCNO>" in line :
                doc_no = re.sub('<[/]*\w+>', '', line)
                doc_no = doc_no.strip(" \n")
                line = f.readline()
                text = ""
                while line and "</DOC>" not in line :
            
                    if (line == '\n') or ("<!--" in line) :
                        line = f.readline()
                        continue

                    line=re.sub('<[/]*\w+>[\n]*', '', line)
                    if filename[0:2]=="la":
                        line=re.sub('<[TABLECELL].*>', '', line)
                    elif filename[0:2]=="fb":
                        line=re.sub('<[F P=]*[0-9]*>', '', line)
                        line=re.sub('<[FIG ID=]*.*[A-D]>', '', line)
                        
                    text+=line
                    line = f.readline()
                indexMovie(doc_no, text)
            line = f.readline()


makeIndex(collection)
closeWriter()
