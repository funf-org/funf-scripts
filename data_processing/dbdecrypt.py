#!/usr/bin/env python
'''Decrypt one or more sqlite3 files using the provided key.  Checks to see if it is readable
'''
from optparse import OptionParser
import decrypt
import sqlite3
import shutil

_random_table_name = 'jioanewvoiandoasdjf'
def is_funf_database(file_name):
    try:
        conn = sqlite3.connect(file_name)
        conn.execute('create table %s (nothing text)' % _random_table_name)
        conn.execute('drop table %s' % _random_table_name)
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return False
    else:
        return True
    finally:
        if conn is not None: conn.close()

def decrypt_if_not_db_file(file_name, extension=None):
    if is_funf_database(file_name):
        print "Already decrypted: '%s'" % file_name
    else:
        print ("Attempting to decrypt: '%s'..." % file_name),
        decrypt.decrypt([file_name], extension)
        if is_funf_database(file_name):
            print "Success!"
        else:
            print "FAILED!!!!"
            print "File is either encrypted with another method, another key, or is not a valid sqlite3 db file."
            print "Keeping original file."
            shutil.move(file_name + "." + extension, file_name)

if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options] [sqlite_file1.db [sqlite_file2.db...]]")
    parser.add_option("-i", "--inplace", dest="extension", default=None,
                      help="The extension to rename the original file to.  Will not overwrite file if it already exists. Defaults to '%s'." % decrypt.default_extension,)
    (options, args) = parser.parse_args()
    for file_name in args:
        decrypt_if_not_db_file(file_name, options.extension)