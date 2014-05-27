fasts3plusglacier
=================

# backup and restore files to S3 and Glacier

This utility is targeted towards Stanford's research computing community requirements.  Namely, how do you efficiently backup (both for cost and time) a filesystem which is composed of:

* contains a mix of file sizes from a few kilobytes to single digit terabyte.
* small kilobyte sized files may number in the 100-200 million range
* filesystem size may be as small at 50TB and as large as a few petabytes

This sript uses S3 and lifecycle management to move things to and from Glacier.  As such:

* small files are cheaper to keep in S3 than glacier
* large files are cheaper to keep in Glacier than S3


## upload strategy

The upload strategy is same as the fasts3v1 script.  Namely

With increasing number of streams, generally speaking networks get
faster. With increasing number of streams, generally speaking mechanical
disks get slower.  Hence the performance curves have opposite
slopes. Where they cross is the performance you see.

This script attempts to make use of memory buffers to make the storage curve behave more like the network.

Theory of operation:

Use a single disk stream to fill buffers. Once a buffer is full, the
multipart upload is kicked off and the disk read is picked up by another
buffer. As long there is an available buffer then the disk is read
continuously. Technically the disk is ready by many threads, but some care
is taken to make sure only one thread is reading at any one time.

The end result is the disk is read sequentially, the network in parallel, and the two are largely asynchronous to each other.


## Modes of operation


### backup

```python fasts3plusglacier.py --bucketname myawsbucket --backup $PWD```

Recursively walk the current directory and upload any files to S3 which are not present or whos modification times do not match.


### restore

```python fasts3plusglacier.py --bucketname myawsbucket --restore $PWD```

Recursively restore the files from the path $PWD in S3.  The files will be placed into a subdirectory called glacierrestore.

### compare filesystem to S3

```python fasts3plusglacier.py --bucketname myawsbucket $PWD```

Recursively compare the files in current directory to those in S3.  By default the modification time and filesize are compared.  There is an option to compare based on the sha256 hash tree algorithm.


## all command line options:

```
fasts3plusglacier.py --help
usage: fasts3plusglacier.py [-h] [--restore] [--backup] [--show-bucket-policy]
                            [--check-sha256] --bucketname BUCKETNAME
                            [--verbose]
                            [--num-smallfile-workers NUM_SMALLFILE_WORKERS]
                            [--num-largefile-workers NUM_LARGEFILE_WORKERS]
                            [--num-largefile-buffers NUM_LARGEFILE_BUFFERS]
                            [directories [directories ...]]

Research Computing S3/Glacier backups

positional arguments:
  directories           dirs to scan

optional arguments:
  -h, --help            show this help message and exit
  --restore             restore given directory from Glacier
  --backup              backup files in listed directories to S3/Glacier
  --show-bucket-policy  show bucket policy for moving files to glacier
  --check-sha256        when comparing, use sha hash instead of mtime
  --bucketname BUCKETNAME
                        which bucket to use
  --verbose             verbose output
  --num-smallfile-workers NUM_SMALLFILE_WORKERS
                        number of threads to upload small files
  --num-largefile-workers NUM_LARGEFILE_WORKERS
                        number of threads to upload large files
  --num-largefile-buffers NUM_LARGEFILE_BUFFERS
                        number of buffers used by largefile threads. each
                        buffer is 40MB.
```

