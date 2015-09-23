import hashlib

def hashword16(word, string=True):
    '''
    produces a 16 bit hash either in base16 string or int
    '''
    md5 = hashlib.md5()
    md5.update(word)
    word_hash = md5.hexdigest()
    if string:
        return word_hash[0:4]
    else:
        return int(word_hash[0:4], 16)

def hashword32int(word):
    '''
    produces a 32 bit hash integer
    '''
    md5 = hashlib.md5()
    md5.update(word)
    word_hash = md5.hexdigest()
    return int(word_hash, 16) % 4294967296 # 2**32

