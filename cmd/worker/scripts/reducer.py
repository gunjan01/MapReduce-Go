import re
import os
import uuid
import json
import sys

#Usage python3 reducer.py ./scripts/8080 8080

def reducer(path, dir):
    reducerList = {}

    for file in dir:
        with open(path + '/' + file, 'r') as myfile:
            data = myfile.read()

        obj = json.loads(data)

        for i in obj:
            if i not in reducerList:
                reducerList[i] = 0
            reducerList[i] = reducerList[i] + len(obj[i])
        
    return json.dumps(reducerList, indent=4, sort_keys=True)
    
if __name__ == "__main__":
    path = sys.argv[1]
    dir = os.listdir(path)
    if len(dir) > 0:
        reducer_output = reducer(path, dir)
        print(reducer_output)


