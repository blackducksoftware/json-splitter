'''
Created on Nov 15, 2022
@author: kumykov

BDIO Splitter allows to split BDIO files containing scan graph produced by Signature Scanner version 8.x

The process is as following:

uncompress original file into the source directory:
It should produce a dataset looking like following:

```
source-folder
|- bdio-header.jsonld
|- bdio-entry-00.jsonld
. . .
```

Set datadir variable to the source folder
Set outdir variable to the destination folder

Invoke as following:

python3 split_scan_graph_bdio.py

Once completed outdir will contain a set of folders

outdir
|- _part00
|- _part01
. . .

Each containing a set of **jsonld** files that will make a valid BDIO document

Compress them by executing the following command in the outdir folder:

for i in _* ; do echo $i ; (cd $i ; zip ../${i}.bdio bdio-header.jsonld bdio-entry-* ) ; done

this will produce a set of files as:

_part00.bdio
_part01.bdio
. . .

Each of them could be uploaded and processed individually

# TODO

* Add command line processing
* Add file size computation for bdio-entry-xx.jsonld files
* Add Project name and version override options

'''
import json
import os
from pprint import pprint
import uuid
import copy

p_key = 'https://blackducksoftware.github.io/bdio#hasParentId'

def load_data(datadir):
  content = dict()
  for filename in os.listdir(datadir):
    fpath = os.path.join(datadir, filename)
    if os.path.isfile(fpath):
      with open(fpath) as f:
        content[filename] = json.load(f)
  return content

def concatenate_graph(content):
  graph = []
  for key in content.keys():
    graph.extend(content[key]['@graph'])
  return graph

def sorted_file_graph(graph):
  graph_entries_with_parent = [d for d in graph if d.get(p_key)]
  sorted_graph = sorted(graph_entries_with_parent, key=lambda x: x[p_key][0]['@value'])
  return sorted_graph

def non_file_graph(graph):
  return [d for d in graph if not d.get(p_key)]

def write_header(outdir, header):
  headerfilename = 'bdio-header.jsonld'
  if not os.path.exists(outdir):
    os.makedirs(outdir)
  headerfilepath = os.path.join(outdir,headerfilename)
  with open(headerfilepath,"w") as f:
    json.dump(header,f, indent=1)

def write_entry_file(outdir, header, project_entry, graph):
  entry_object = dict()
  entry_object['@id'] = header['@id']
  entry_object['@type'] = header['@type']
  offset=0
  size=5000
  entry_number=0
  current_slice = graph[offset:size]
  while len(current_slice) > 0:
    entry_object['@graph'] = current_slice
    entry_object['@graph'].insert(0, project_entry)
    entryfilename = 'bdio-entry-{:02d}.jsonld'.format(entry_number)
    if not os.path.exists(outdir):
      os.makedirs(outdir)
    entryfilepath = os.path.join(outdir,entryfilename)
    with open(entryfilepath,"w") as f:
      json.dump(entry_object,f, indent=1)
    entry_number += 1
    current_slice = graph[size*entry_number:size*(entry_number+1)]
  
def update_header(header, part_name, part_uuid):
  header['@id'] = part_uuid
  name = header['https://blackducksoftware.github.io/bdio#hasName'][0]['@value']
  index = name.index(' signature')
  updated_name = name[:index] + part_name + name[index:]
  header['https://blackducksoftware.github.io/bdio#hasName'][0]['@value'] = updated_name

# Input data directory uncompress original BDIO file here
datadir = "../jsonld"
# Output folder for results
outdir = "../jsonldout"
max_file_entries = 150000
content = load_data(datadir)
header = content['bdio-header.jsonld']
graph = concatenate_graph(content)
graph_entries_with_no_parent = non_file_graph(graph)
sorted_graph = sorted_file_graph(graph)
num_source_entries = len(sorted_graph)
sorted_parent_ids = [id[p_key][0]['@value'] for id in sorted_graph]
sorted_node_ids = [int(id['@id'][id['@id'].index('scanNode-')+9:1000]) for id in sorted_graph]

offset=0
size=max_file_entries
part=0
current_chunk = sorted_graph[size*part:size*(part+1)]
while len(current_chunk) > 0:
  parent_ids = [id[p_key][0]['@value'] for id in current_chunk]
  node_ids = [int(id['@id'][id['@id'].index('scanNode-')+9:1000]) for id in current_chunk]
  # missing_node_ids = list(set(node_ids).difference(parent_ids))
  missing_parent_ids = list(set(parent_ids).difference(node_ids))
  print (missing_parent_ids)
  if -1 in missing_parent_ids:
    missing_parent_ids.remove(-1)
  backfill = []
  for parent_id in missing_parent_ids:
    # print (sorted_graph[sorted_node_ids.index(parent_id)])
    while parent_id > 0:
      parent = sorted_graph[sorted_node_ids.index(parent_id)]
      # pprint(parent_id)
      parent_id = parent[p_key][0]['@value']
      if parent_id not in backfill:
        backfill.append(parent_id)
  backfill.extend(missing_parent_ids)
  print (backfill)
  for i in backfill:
    current_chunk.append(sorted_graph[sorted_node_ids.index(i)])
  part_name = "_part{:02d}".format(part)
  part_uuid = uuid.uuid4().urn
  header_copy = copy.deepcopy(header)
  update_header(header_copy, part_name, part_uuid)
  output_path = os.path.join(outdir, part_name)
  write_header(output_path, header_copy)
  write_entry_file(output_path, header_copy, graph_entries_with_no_parent[0], current_chunk)
  part += 1
  current_chunk = sorted_graph[size*part:size*(part+1)]
