# mumbler

## Usage
```
cd /root/mumbler
./mumbler.py <word> <depth>
```
For example:
```
./mumbler.py master 10
master of the members and many times . The problem
```

## Design

### Preprocess

Each input file from google 2gram is preprossed, 2gram counts are aggregated overal all years, and other information we don't need are thrown away. The output file of is stored in `/gpfs/gpfsfpo`. Each input file is mapped exactly into one data file (this turned out to be a big mistake, more on this later). The output files are plain text files named `gram2_<index>.processed`, structed as such:
```
-[<parent word>]- TAB <total child words number> TAB <total child words count>
<child_word> TAB <count>
<child_word> TAB <count>
...
```

The idea was when reading the data file line by line, we can identify special line for parent word which starts with `"-["`, parse it to get parent words are related per file meta data (number of child words and total child words count). If the parent word is not one we are interested in, we can simply skip n lines ahead, with n as the count of child words. 

Each data file ended up being about 16MB, and there are exactly 100 of them. 

### Speeding up attempt 1

Initially, I had the wrong assumption about input files. I had thought for a given word, say "hello", its data will only be stored in a small subset of all files (for instance, only file 2,10,40 contains the word out of all 100). However, soon enough I discovered this is not the case. 

With this wrong assumption, I had pre-generated a boomfilter file for each processed file, with the idea being, at runtime we can simply load the bloomfilter into memory, and efficiently check if a given file contains the word we are interested in and only examine the files tha do contain it. 

It turns out, a lot of words are contained by all 100 files! And mumbler runtime performance was abysmal - as we had to read all 100 files to look up the next words at each single step!

### Speeding up attempt 2

So I gave up on the bloomfilter idea and instead focused on building a index that maps a given parent word to its byte location in all 100 files. 

The idea was given a parent word, we have a pre-built index that stores up to 100 integers  which serve as pointers for where to find its data in all processed data files. In addition, each parent word stores another number per data file which is a byte count for all its data. The data for a given file can be loaded more efficiently, for example:
```
...
file.seek(starting_position)
file.read(chunk_size)
...
```

The appoach improved the runtime performance quite a bit, the mumbler now runs within minutes. 

