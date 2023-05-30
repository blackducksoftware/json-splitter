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

Invoke as following:
python3 split_scan_graph_bdio.py -in source-folder -out outdir

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

Command line options:

options:
  -h, --help            show this help message and exit
  -in INPUT_DIR, --input-dir INPUT_DIR
                        Location for uncompressed source BDIO file
  -out OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Lokation for generated BDIO filesets
  -mn MAX_FILE_ENTRIES, --max-file-entries MAX_FILE_ENTRIES
                        Maximum scan node entries per generated BDIO file
  -mcn MAX_CHUNK_NODES, --max-chunk-nodes MAX_CHUNK_NODES
                        Maximum scan node entries per single bdio-entry file
  -pn PROJECT_NAME, --project-name PROJECT_NAME
                        Change project name
  -pv PROJECT_VERSION, --project-version PROJECT_VERSION
                        Change project version

# TODO

* Add file size computation for bdio-entry-xx.jsonld files

'''
import json
import os
from pprint import pprint
import uuid
import copy
import argparse

p_key = 'https://blackducksoftware.github.io/bdio#hasParentId'
type_project = "https://blackducksoftware.github.io/bdio#Project"
type_name = "https://blackducksoftware.github.io/bdio#hasName"
type_version = "https://blackducksoftware.github.io/bdio#hasVersion"

max_nodes_per_entry_file=5000

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
  size=max_nodes_per_entry_file
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

def update_project_name_version(project_entry, new_project_name, new_project_version):
  entry_type = project_entry['@type']
  if entry_type == type_project:
    return
  if new_project_name:
    project_entry[type_name][0]['@value'] = new_project_name
  if new_project_version:
    project_entry[type_version][0]['@value'] = new_project_version


parser = argparse.ArgumentParser("Split BDIO file into smaller chunks to facilitate processing of large scans")
parser.add_argument("-in", "--input-dir", required=True, help="Location for uncompressed source BDIO file")
parser.add_argument("-out", "--output-dir", required=True, help="Lokation for generated BDIO filesets")
parser.add_argument("-mn", "--max-file-entries", default=100000, type=int, help="Maximum scan node entries per generated BDIO file")
parser.add_argument("-mcn", "--max-chunk-nodes", default=3000, type=int, help="Maximum scan node entries per single bdio-entry file")
parser.add_argument("-pn", "--project-name", default=None, help="Change project name")
parser.add_argument("-pv", "--project-version", default=None, help="Change project version")
args = parser.parse_args()

pprint (args)
# quit()

# Input data directory uncompress original BDIO file here
datadir = args.input_dir
outdir = args.output_dir
max_file_entries = int(args.max_file_entries)
max_nodes_per_entry_file = args.max_chunk_nodes
new_project_name = args.project_name
new_project_version = args.project_version

# validate input directory
if not os.path.exists(datadir):
  print (f"\nInput directory {datadir} was not found\n Exiting...\n")
  quit(1)


content = load_data(datadir)
header = content['bdio-header.jsonld']
graph = concatenate_graph(content)
graph_entries_with_no_parent = non_file_graph(graph)
sorted_graph = sorted_file_graph(graph)
num_source_entries = len(sorted_graph)
sorted_parent_ids = [int(id[p_key][0]['@value']) for id in sorted_graph]
sorted_node_ids = [int(id['@id'][id['@id'].index('scanNode-')+9:1000]) for id in sorted_graph]

offset=0
size=max_file_entries
part=0
first_part = max_nodes_per_entry_file*2
current_chunk = sorted_graph[:first_part]
while len(current_chunk) > 0:
  print (len(current_chunk))
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
  if new_project_version:
    part_name = f"_{new_project_version}{part_name}"
  if new_project_name:
    part_name = f"_{new_project_name}{part_name}"
  part_uuid = uuid.uuid4().urn
  header_copy = copy.deepcopy(header)
  project_entry_copy = copy.deepcopy(graph_entries_with_no_parent[0])
  update_header(header_copy, part_name, part_uuid)
  update_project_name_version(project_entry_copy, new_project_name, new_project_version)
  output_path = os.path.join(outdir, part_name)
  write_header(output_path, header_copy)
  write_entry_file(output_path, header_copy, project_entry_copy, current_chunk)
  current_chunk = sorted_graph[size*part+first_part:size*(part+1)+first_part]
  part += 1
