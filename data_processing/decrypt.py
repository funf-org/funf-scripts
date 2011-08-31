#!/usr/bin/env python
'''Decrypt one or more files using the provided key
'''
from optparse import OptionParser
import shutil
import os.path
from Crypto.Cipher import DES
import string

_des_key = string.join([chr(byte) for byte in (12,34,45,54,27,122,33,45)], '')
default_extension = "orig"

def decrypt(file_names, extension=None, key=None):
    extension = extension or default_extension
    key = key or _des_key
    decryptor = DES.new(key)
    for file_name in file_names:
        with open(file_name) as file:
            encrypted_data = file.read()
            data = decryptor.decrypt(encrypted_data)
        if not os.path.exists(file_name + '.' + extension):
            shutil.copy2(file_name, file_name + '.' + extension)
        with open(file_name, 'w') as file:
            file.write(data)
        
        

if __name__ == '__main__':
    usage = "%prog [options] [file1 [file2...]]"
    description = "Decrypts files using the DES key specified, or the one included in this script.  Keeps a backup copy of the original file.  \nWARNING: This script does not detect if a file has already been decrypted.  \nDecrypting a file that is not encrypted will scramble the file."
    parser = OptionParser(usage="%s\n\n%s" % (usage, description))
    parser.add_option("-i", "--inplace", dest="extension", default=None,
                      help="The extension to rename the original file to.  Will not overwrite file if it already exists. Defaults to '%s'." % default_extension,)
    parser.add_option("-k", "--key", dest="key", default=None,
                      help="The DES key used to decrypt the files.  Uses the default hard coded one if one is not supplied.",)
    (options, args) = parser.parse_args()
    decrypt(args, options.extension)