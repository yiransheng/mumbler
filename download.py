import os
import sys
import urllib2

import node_manager

goog_url = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-%s.csv.zip"


def download_all(force=False):
    index_offset = node_manager.local_node.index_offset
    nnodes = node_manager.local_node.count()
    for i in range(index_offset, 100, nnodes):
        download_2gram_by_index(i, force=force)

def download_2gram_by_index(index, dest=node_manager.GPFSNodeManager.local_storage, force=False):
    dest_zip_file = os.path.join(dest, "gram2_" + str(index) + ".csv.zip")
    url = goog_url % str(index)
    if force or os.path.isfile(dest_zip_file):
        print "Downloading from: \n [%s]" % url
        download_file = dest_zip_file + ".download"
        input_req = urllib2.urlopen(url)
        with open(download_file, 'w') as download_writer:
            while True:
                chunk = input_req.read(1 << 14)  # 16 KiBi chunks
                if not chunk:
                    break
                download_writer.write(chunk)
        os.rename(download_file, dest_zip_file)
        print "done."
    else:
        print "skipped url: \n [%s]" % url

if __name__ == '__main__':
    force = sys.argv[1] == "--force" if sys.argv[1] else False
    download_all(force)


