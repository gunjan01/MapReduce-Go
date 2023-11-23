import re
import os
import uuid
import json
import sys

def mapper(filename):
    mapperList = {}

    with open(filename) as f:
        lines = f.readlines()
        for line in lines:
            wordList = re.sub("[^\w]", " ",  line).split()
            for word in wordList:
                word = word.lower()
                if word not in mapperList:
                    mapperList[word] = []
                mapperList[word].append(1)

    return json.dumps(mapperList)

if __name__ == "__main__":
    mapper_output = mapper(sys.argv[1])
    print(mapper_output)