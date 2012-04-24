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

'''Decrypt one or more files using the provided key
'''
from optparse import OptionParser
import shutil
import os.path
from Crypto.Cipher import DES
import struct
import string
import tempfile

default_password = 'changeme'
default_extension = "orig"

_iterations = 135
_salt = '\xa6\xab\x09\x93\xf4\xcc\xee\x10'
_key_size = 8
_block_size = 8

def key_from_password(password, salt=_salt, iterations=_iterations):
    '''Imitate java's PBEWithMD5AndDES algorithm to produce a DES key'''
    from Crypto.Hash import MD5
    hasher = MD5.new()
    hasher.update(password)
    hasher.update(salt)
    result = hasher.digest()
    for i in range(1, iterations):
        hasher = MD5.new()
        hasher.update(result)
        result = hasher.digest()
        #test = ' '.join([str( unsigned ) for unsigned in [ord(character) for character in result]])
        #print test

    key = result[:8]
    

    # TODO: Not likely, but may need to adjust for twos complement in java

    #For DES keys, LSB is odd parity for the key
    def set_parity(v):
        def num1s_notlsb(x):
            return sum( [x&(1<<i)>0 for i in range(1, 8)] )
        def even_parity(x):
            return num1s_notlsb(x)%2 == 0
        return v|0b1 if even_parity(v) else v&0b11111110
    return ''.join([chr(set_parity(ord(digit))) for digit in key]) 
    
def prompt_for_password():
    from getpass import getpass
    return getpass("Enter encryption password: ")

def backup_file(file_name, extension=None):
    extension = extension or default_extension
    return file_name + '.' + extension

def remove_padding(data):
    data_size = len(data)
    if data_size == 0:
        return data
    num_padding_bytes = struct.unpack('B', data[data_size - 1])[0]
    if num_padding_bytes > 8:
        return data
    return data[:(data_size - num_padding_bytes)]

def iter_has_next(iterable): 
    it = iter(iterable) 
    current = it.next()
    for i in it: 
        yield current, True
        current = i
    yield current, False

def decrypt(file_names, key, extension=None):
    assert key != None
    decryptor = DES.new(key)
    for file_name in file_names:
        
        # Iteratively read 8 byte blocks, decrypt, and write to temp file
        with open(file_name, 'rb') as encrypted_file:
            with tempfile.NamedTemporaryFile(delete=False) as output_file:
                for chunk, has_next in iter_has_next(iter((lambda:encrypted_file.read(_block_size)),'')):
                    data = decryptor.decrypt(chunk)
                    if not has_next:
                        data = remove_padding(data)
                    output_file.write(data)
        
        # When successful backup original file, and move temp file to original location
        backup_file_name = backup_file(file_name, extension)
        if not os.path.exists(backup_file_name):
            shutil.move(file_name, backup_file_name);
        shutil.move(output_file.name, file_name);
        
        

if __name__ == '__main__':
    usage = "%prog [options] [file1 [file2...]]"
    description = "Decrypts files using the DES key specified, or the one included in this script.  Keeps a backup copy of the original file.  \nWARNING: This script does not detect if a file has already been decrypted.  \nDecrypting a file that is not encrypted will scramble the file."
    parser = OptionParser(usage="%s\n\n%s" % (usage, description))
    parser.add_option("-i", "--inplace", dest="extension", default=None,
                      help="The extension to rename the original file to.  Will not overwrite file if it already exists. Defaults to '%s'." % default_extension,)
    parser.add_option("-k", "--key", dest="key", default=None,
                      help="The DES key used to decrypt the files.  Uses the default hard coded one if one is not supplied.",)

    (options, args) = parser.parse_args()
    key = options.key if options.key else key_from_password(prompt_for_password())
    decrypt(args, key, options.extension)