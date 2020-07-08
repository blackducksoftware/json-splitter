'''
Created on Dec 13, 2018

@author: kumykov
'''

import json
from warnings import catch_warnings
from sys import argv, version_info, exit
from collections import OrderedDict

def load_json_file(filename):
    with open(filename) as f:
        data = json.load(f, object_pairs_hook=OrderedDict)
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
print (version_info.major)
if version_info.major < 3:
    print ("Python 3 is required, exiting")
    exit (1)

print ("Runnign Python {}.{}.{}".format(version_info.major, version_info.minor, version_info.micro))
print ("processing %s " % argv[1])

scanData  = load_json_file(argv[1])
dataLength = len(scanData['scanNodeList'])
scanSize = sum(node['size'] for node in scanData['scanNodeList'] if node['uri'].startswith("file://"))

print ("Number of scan entries %s" % dataLength)
print ("Total size of the scan %s" % scanSize)

print( scanData.keys())

# Establishing two limiting factors, number of node list entries and size of the scan
# Size of the scan includes file objects only, files within archive files are not counted.
#
maxNodeEntries = 200000
maxScanSize = 4500000000

scanData['project'] = scanData['project'] + "-more"
scanName = scanData['name']
baseDir = scanData['baseDir']
scanNodeList = scanData.pop('scanNodeList')
scanData.pop('scanProblemList')
scanData['scanProblemList'] = []
base = scanNodeList[0]

print(base)

# Computing split points for the file
#
scanChunkSize = 0
scanChunkNodes = 0
splitAt = [0]
for i in range(0, dataLength-1):
    if scanChunkSize + scanNodeList[i+1]['size'] > maxScanSize or scanChunkNodes + 1 > maxNodeEntries:
        scanChunkSize = 0
        scanChunkNodes = 0
        splitAt.append(i) 
    if scanNodeList[i]['uri'].startswith('file://'):
        scanChunkSize = scanChunkSize + scanNodeList[i]['size']
    scanChunkNodes += 1
    
# Create array of split points shifting by one position
splitTo = splitAt[1:]
splitTo.append(None)

print(splitAt)
print(splitTo)


# Splitting and writing the chunks
#

for i in range(len(splitAt)):
    print ("Processing range {}, {}".format(splitAt[i], splitTo[i]))
#for i in range(0, dataLength, maxNodeEntries):
    nodeData = scanNodeList[splitAt[i]:splitTo[i]]
    if i > 0:
        nodeData.insert(0,base)
    # scanData['baseDir'] = baseDir + "-" + str(i)
    scanData['scanNodeList'] = nodeData
    scanData['name'] = scanName + "-" + str(splitAt[i])
    filename = argv[1] + "-" + str(splitAt[i]) + '.json'
    write_json_file(scanData, filename)
    scanData.pop('scanNodeList')

print("Done.")