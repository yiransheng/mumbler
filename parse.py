import os
from collections import Counter

def extract_next_words(word, datafile):
    assert os.path.isfile(datafile)
    words = Counter()
    counts = 0
    if not word or len(word) == 0:
        return Counter(), counts

    with open(datafile, 'r') as df:
        line = next(df)
        try:
            while True:
                if is_parent_line(line):
                    current_word, nchildren, counts = parse_parent_line(line)
                    if word == current_word:
                        for i in xrange(1, nchildren):
                            child_word, count = parse_child_line(next(df))
                            words[child_word] = count
                        break
                    else:
                        for i in xrange(1, nchildren):
                            next(df)
                line = next(df)
        except StopIteration:
            pass

    return words, counts

def parse_child_line(line):
    tokens = line.split("\t")
    if len(tokens) == 2:
        word = tokens[0].replace("\\-\\[", "-[").replace("\\]\\-", "]-")
        count = int(tokens[1])
        return word, count


def parse_parent_line(line):
    tokens = line.split("\t")
    if len(tokens) == 3:
        word_token = tokens[0]
        child_count = int(tokens[1])
        child_occurs = int(tokens[2])
        l = len(word_token)
        word = word_token[2:l-2] \
                .replace("\\-\\[", "-[").replace("\\]\\-", "]-")
        return word, child_count, child_occurs

def is_parent_line(line):
    return len(line)>0 and line.startswith("-[")

