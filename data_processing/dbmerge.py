#!/usr/bin/env python
'''Merges data from a group of Funf sqlite files into one CSV file per table.
'''
import sqlite3
from optparse import OptionParser
import os.path
import time

file_info_table = 'file_info'
data_table = 'data'

def merge(db_files=None, out_file=None):
    # Check that db_files are specified and exist
    if not db_files:
        db_files = [file for file in os.listdir(os.curdir) if file.endswith(".db")]
        if not db_files: 
            raise Exception("Must specify at least one db file")
    nonexistent_files = [file for file in db_files if not os.path.exists(file)]
    if nonexistent_files:
        raise Exception("The following db files do not exist: %s" % nonexistent_files)
    
    # Use default filename if it doesn't ixist
    if not out_file:
        out_file = 'merged_%d.db' % int(time.time())
    
    if os.path.exists(out_file):
        raise Exception("The file '%s' already exists." % out_file)
    
    out_conn = sqlite3.connect(out_file)
    out_conn.row_factory = sqlite3.Row
    out_cursor = out_conn.cursor()
    
    out_cursor.execute('create table data (id text, device text, probe text, timestamp long, value text)')
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try: 
            cursor.execute("select * from %s" % file_info_table)
        except sqlite3.OperationalError:
            print "Unable to parse file: " + db_file
            continue
        else:
            try:
                for row in cursor:
                    id, device, uuid, created = row
            except IndexError:
                print "No file info exists in: " + db_file
                continue
            print "Processing %s" % db_file
            cursor.execute("select * from %s" % data_table)
            for row in cursor:
                id, probe, timestamp, value = row
                new_row = (('%s-%d' % (uuid, id)), device, probe, timestamp, value)
                out_conn.execute("insert into data values (?, ?, ?, ?, ?)", new_row)
            out_conn.commit()
    out_cursor.close()


if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options] [sqlite_file1.db [sqlite_file2.db...]]")
    parser.add_option("-o", "--output", dest="file", default=None,
                      help="Filename to merge all files into.  Created if it doesn't exist.", metavar="FILE")
    (options, args) = parser.parse_args()
    try:
        merge(args, options.file)
    except Exception as e:
        import sys
        sys.exit("ERROR: " + str(e))