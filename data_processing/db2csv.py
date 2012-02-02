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
from optparse import OptionParser
import sqlite3
import simplejson as json
import csv
from itertools import product, groupby

# Two passes, one to figure out the keys for each probe name, the 2nd to fill out the values

def _inner_flatten_values(value, prefix=None):
    '''Takes an arbitrarily deep dict and returns a list of flat dictionaries, 

    The number of dicts in the list depends on the number of lists in the value heirarchy'''
    prefix = prefix or ''
    if isinstance(value, dict):
        # TODO May want full paths, instead of list truncated ones
        # Current code assumes that list names are not relevant to structure
        if prefix:
            items = [(prefix if isinstance(val, list) else '%s_%s' % (prefix, key), val) for key, val in value.items()]
        else:
            items = [(prefix if isinstance(val, list) else key, val) for key, val in value.items()]
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
        return  [flattened_val for val in value for flattened_val in _inner_flatten_values(val)]   # Enumerates list with prefix 
    else:
        return [{prefix:value}]
    

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


def convert(db_file):
    csv.DictWriter
    
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try: 
        cursor.execute("select * from data limit 100 ")
    except (sqlite3.OperationalError,sqlite3.DatabaseError):
        raise Exception("Unable to parse file: " + db_file)
    else:
        try:
            for row in cursor:
                _id, device, probe, timestamp, value = row
                print value
                print json.dumps(flatten_values(json.loads(value)), sort_keys=True, indent=4)
        except IndexError:
            raise Exception("No file info exists in: " + db_file)
        print "Processing %s" % db_file
        
    pass

if __name__ == '__main__':
    usage = "%prog [options] [sqlite_file1.db [sqlite_file2.db...]]"
    description = __doc__
    parser = OptionParser(usage="%s\n\n%s" % (usage, description))
    (options, args) = parser.parse_args()
    for file_name in args:
        convert(file_name)