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

_iterations = 135
_salt = '\xa6\xab\x09\x93\xf4\xcc\xee\x10'

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

def decrypt(file_names, extension=None, key=None):
    extension = extension or default_extension
    key = key or key_from_password(prompt_for_password())
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