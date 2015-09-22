import os
import zipfile

from contextlib import nested, closing
from collections import Counter


import node_manager

def process_all():
    index_offset = node_manager.local_node.index_offset
    nnodes = node_manager.local_node.count()
    for index in range(index_offset, 100, nnodes):
        filename = "gram2_" + str(index)
        zip_file = os.path.join(node_manager.GPFSNodeManager.local_storage,
                                filename + ".csv.zip")
        out_file = os.path.join(node_manager.GPFSNodeManager.gpfs_storage, filename + ".processed")
        if os.path.isfile(zip_file) and not os.path.isfile(out_file):
            process_zip(zip_file, out_file)
        elif not os.path.isfile(zip_file):
            print("Need to download the zip file first.")
        else:
            print("Already processed.")

def process_zip(infile, outfile):
    '''
    Takes a single zip file, and outputs processed ngram data file for use later
    a block for a single parent word in outfile looks like:
    # [parent_word] TAB <number of child_words> TAB <total counts across all years for all child words>
    -[financial]- 2 11
    analysis 6
    straits 5

    lines are spit out one at a time

    parts on each line are seperated by "\t"
    we store the number of childwords (lines in the following block in the anchor line,
    so that we can skip ahead if the current parent word is not the one we are interested in, when reading
    the file
    '''
    print("unzip file %s" % infile)

    with closing(zipfile.ZipFile(infile)) as gram2_zip:
        gram2_filename = gram2_zip.namelist()[0]
        it = process_line()
        it.next()
        with nested(gram2_zip.open(gram2_filename), open(outfile, 'w')) as (gram2_file, gram2_out):
            for line in gram2_file:
                x = it.send(line)
                if x is not None:
                    it_out_block = produce_parent_word_block(*x)
                    for out_line in it_out_block:
                        gram2_out.write(out_line)
                        gram2_out.write("\n")

            # send a "EOF" signal to the process_line iterator
            x = it.send(None)    # indicating all lines have been processed grab last parent word
            if x is not None:
                it_out_block = produce_parent_word_block(*x)
                for out_line in it_out_block:
                    gram2_out.write(out_line)
                    gram2_out.write("\n")
    print("done")


def strip_unicode_and_special(word):
    '''
    Replaces unicode chars and escape "-[" and "]-" (which we use to encode parent word) just in case
    '''
    word = word.encode('UTF-8', 'replace') if isinstance(word, unicode) else word
    return word.replace("-[", "\\-\\[").replace("]-", "\\]\\-")

def process_line(eof=None):
    '''
    A generator that processes raw 2gram file line by line
    Takes advantage of raw date file being sorted alphabettically, reduces memory footprint drastically
    Operates on a "parent word" at a time
    if encounters a new parent word (first word in 2 gram), yields the following:
    (parent_word::String, dict of child words count:: Counter)

    output of the iterator can be immediately written to another file, only require a few MB memory at most
    (depending on how many child words a given parent word has in the input file)
    '''
    current_parent_word = None  # the parent word being processed, we take advantage of 2gram data are sorted per file
    current_parent_dict = None  # a counter with child/second word as key, and occurences as value

    prev_parent_word = None
    prev_parent_dict = None

    while True:
        line = yield (prev_parent_word, prev_parent_dict) \
            if prev_parent_word is not None else None

        if line is not eof:
            tokens = line.split('\t')
            if len(tokens) >= 3:
                two_gram = tokens[0].strip().split()
                count = tokens[2]
                if len(two_gram) == 2:
                    parent = strip_unicode_and_special(two_gram[0])
                    child = strip_unicode_and_special(two_gram[1])
                    if parent == current_parent_word:
                        current_parent_dict[child] += int(count)
                        prev_parent_word = None
                        prev_parent_dict = None
                        prev_children_count = 0
                    else:
                        prev_parent_word = current_parent_word
                        prev_parent_dict = current_parent_dict
                        current_parent_word = parent
                        current_parent_dict = Counter()
                        current_parent_dict[child] = int(count)
                        # print("New word encountered: %s" % parent)
                else:
                    # print("Not a 2gram.")
                    pass
            else:
                # print("Unrecognized line input.")
                pass
        else:
            prev_parent_word = current_parent_word
            prev_parent_dict = current_parent_dict
            yield (prev_parent_word, prev_parent_dict) \
                if prev_parent_word is not None else None
            break


def produce_parent_word_block(word, counter, sep="\t"):
    '''
    If fed the output from process_line, this generator spits out a block of 2gram info for
    a parent word, which can be written to processed file, and to be parsed later, the block
    looks like:
    # [parent_word] TAB <number of child_words> TAB <total counts across all years for all child words>
    -[financial]- 2 11
    analysis 6
    straits 5

    lines are spit out one at a time

    parts on each line are seperated by "\t"
    we store the number of childwords (lines in the following block in the anchor line,
    so that we can skip ahead if the current parent word is not the one we are interested in, when reading
    the file
    '''
    nchild_words = len(counter)
    counts = sum(counter.values())
    anchor_line = "-[" + word + "]-" + sep + str(nchild_words) + sep + str(counts)
    yield anchor_line
    for child_word in counter:
        child_line = child_word + sep + str(counter[child_word])
        yield child_line

if __name__ == "__main__":
    process_all()



