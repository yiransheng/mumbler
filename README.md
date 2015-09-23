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

Initially, I had the wrong 

