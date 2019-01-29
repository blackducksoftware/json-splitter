'''
Created on Dec 13, 2018

@author: kumykov
'''

import json
from warnings import catch_warnings
from sys import argv

def load_json_file(filename):
    with open(filename) as f:
        data = json.load(f)
    return data

def write_json_file(data, filename):
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)

def scan_for_missing_parents(data):
    idList = [x['id'] for x in data]
    pIdList = [x.get('parentId', -1) for x in data]
    
    missing = list()
    for i in pIdList:
        if i not in idList:
            if i not in missing:
                missing.append(i)
    return missing

def fix_node_list(data, alldata):
    
    missing = scan_for_missing_parents(data)
    print(missing)
    if len(missing) == 1 and missing[0] == -1:
        print ("Data Is Ok")
        
    else:
        for i in range(0, max(missing)+1):
            data.insert(i,alldata[i])
            

#
# Entry Point
#

print (len(argv))
print ("processing %s " % argv[1])

scanData  = load_json_file(argv[1])
dataLength = len(scanData['scanNodeList'])

print ("Number of scan entries %s" % dataLength)

print( scanData.keys())

splitStep = 200000


print (splitStep)

scanData['project'] = scanData['project'] + "-more"
scanName = scanData['name']
baseDir = scanData['baseDir']
scanNodeList = scanData.pop('scanNodeList')
scanData.pop('scanProblemList')
scanData['scanProblemList'] = []
base = scanNodeList[0]

print(base)

for i in range(0, dataLength, splitStep):
    nodeData = scanNodeList[i:i+splitStep]
    if i > 0:
        nodeData.insert(0,base)
    # scanData['baseDir'] = baseDir + "-" + str(i)
    scanData['scanNodeList'] = nodeData
    scanData['name'] = scanName + "-" + str(i)
    filename = argv[1] + "-" + str(i) + '.json'
    write_json_file(scanData, filename)
    scanData.pop('scanNodeList')
    
    
    


