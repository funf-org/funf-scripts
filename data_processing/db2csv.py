#!/usr/bin/env python
#
# Funf: Open Sensing Framework
# Copyright (C) 2010-2011 Nadav Aharony, Wei Pan, Alex Pentland.
# Acknowledgments: Alan Gardner
# Contact: nadav@media.mit.edu
# 
# This file is part of Funf.
# 
# Funf is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
# 
# Funf is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with Funf. If not, see <http://www.gnu.org/licenses/>.
# 
'''Convert a merged sqlite file with json values into separate csv files per probe
'''
import os.path
from optparse import OptionParser
import sqlite3
import simplejson as json
import csv
from itertools import product, groupby
from collections import defaultdict

# Two passes, one to figure out the keys for each probe name, the 2nd to fill out the values

excluded_keys = ("TIMESTAMP", "PROBE")
def _inner_flatten_values(value, prefix=None):
    '''Takes an arbitrarily deep dict and returns a list of flat dictionaries, 

    The number of dicts in the list depends on the number of lists in the value heirarchy'''
    prefix = prefix or ''
    if isinstance(value, dict):
        if prefix:
            items = [('%s_%s' % (prefix, key), val) for key, val in value.items()]
        else:
            items = value.items()
        flat_values = [{}]
        #print "STARTING"
        for new_prefix, inner_value in items:
            inner_values = _inner_flatten_values(inner_value, prefix=new_prefix)
            # Merge results
            values_product = list(product(flat_values, inner_values))
            if values_product:
                flat_values = [dict(old.items() + new.items()) for old, new in values_product]
        return flat_values
    elif isinstance(value, list):
        return  [flattened_val for val in value for flattened_val in _inner_flatten_values(val, prefix=prefix)]   # Enumerates list with prefix 
    else:
        return  [{}] if prefix in excluded_keys else [{prefix:value}]
    

def flatten_values(value):
    '''Returns a flat list of keys in order for this json value'''
    correlated_index_length = len(value['EVENT_TIMESTAMP']) if ('EVENT_TIMESTAMP' in value) else None
    flat_values = []
    if correlated_index_length:
        correlated_lists = dict([(key, list_value) for key, list_value in value.items() 
                            if (isinstance(list_value, list) and len(list_value) == correlated_index_length)])
        uncorrelated_vals = dict([(key,val) for key,val in value.items() if key not in correlated_lists])
        common_values = _inner_flatten_values(uncorrelated_vals)
        
        # { "X": [1, 3], "Y": [2, 4] }
        # TO
        # [ {"X": 1, "Y": 2},  {"X": 3, "Y": 4}]
        sorted_vals = sorted([(i, key,val) for key, vals in correlated_lists.items() for i, val in enumerate(vals)]) 
        inner_values = [dict([(key, val) for i, key, val in group]) 
                        for index, group in groupby(sorted_vals, lambda x: x[0]) ]
        flat_values = [dict(old.items() + new.items()) for old, new in product(common_values, inner_values)]
    else:
        flat_values = _inner_flatten_values(value)
        
    return flat_values

def get_keys(value):
    return reduce(set.union, [set(fv.keys()) for fv in flatten_values(value)], set())


class keydefaultdict(defaultdict):
    def __missing__(self, key):
        try:
            return self.default_factory(key)
        except TypeError:
            return super(keydefaultdict, self).__missing__(key)

def iterable(some_object):
    try:
        iter(some_object)
        return True
    except TypeError:
        return False


_select_statement = "select * from data"
_builtin_probe_prefix = 'edu.mit.media.funf.probe.builtin.'  

def convert(db_file, out_dir):
    if not out_dir:
        raise Exception("Must specify csv destination out_dir")
    if not os.path.isdir(out_dir):
        if os.path.exists(out_dir):
            raise Exception("File already exists at out_dir path.")
        else:
            os.makedirs(out_dir)
      
    def csv_dict_writer(probe):
        probe = probe.replace(_builtin_probe_prefix, '', 1)
        f = open(os.path.join(out_dir, probe) + ".csv", 'w')
        f.write(u'\ufeff'.encode('utf8')) # BOM (optional...Excel needs it to open UTF-8 file properly)
        return f
    
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    
    probe_to_keys = defaultdict(set)
    cursor = conn.cursor()
    try: 
        cursor.execute(_select_statement)
    except (sqlite3.OperationalError,sqlite3.DatabaseError):
        raise Exception("Unable to parse file: " + db_file)
    else:
        try:
            for row in cursor:
                _id, device, probe, timestamp, value = row
                value_dict = json.loads(value)
                probe_to_keys[probe].update(get_keys(value_dict))
        except IndexError:
            raise Exception("No file info exists in: " + db_file)
    
    
    probe_to_files = keydefaultdict(csv_dict_writer)
    probe_to_writers = {}
    
    cursor = conn.cursor()
    try: 
        cursor.execute(_select_statement)
    except (sqlite3.OperationalError,sqlite3.DatabaseError):
        raise Exception("Unable to parse file: " + db_file)
    else:
        try:
            for row in cursor:
                _id, device, probe, timestamp, value = row
                writer = probe_to_writers.get(probe)
                if not writer:
                    writer = csv.DictWriter(probe_to_files[probe], fieldnames=(["id", "device", "timestamp"] + sorted(probe_to_keys[probe])))
                    writer.writeheader()
                    probe_to_writers[probe] = writer
                basic_info = {"id": _id, "device": device, "timestamp": timestamp}
                value_dict = json.loads(value)
                row_values = flatten_values(value_dict)
                for row in row_values:
                    row.update(basic_info)
                    for k,v in row.items():
                        if isinstance(v, str) or isinstance(v, unicode):
                            row[k] = v.encode('utf8')
                writer.writerows(row_values)
        except IndexError:
            raise Exception("No file info exists in: " + db_file)
    
    for f in probe_to_files.values():
        f.close()

if __name__ == '__main__':
    usage = "%prog [options] [sqlite_file1.db [sqlite_file2.db...]]"
    description = __doc__
    parser = OptionParser(usage="%s\n\n%s" % (usage, description))
    parser.add_option("-o", "--output", dest="output_dir", default=os.curdir,
                      help="Directory write csv files.  Defaults to current directory.", metavar="FILE")
    (options, args) = parser.parse_args()
    try:
        for file_name in args:
            convert(file_name, options.output_dir)
    except Exception as e:
        import sys
        sys.exit("ERROR: " + str(e))