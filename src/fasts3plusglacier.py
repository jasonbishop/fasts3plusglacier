#!/usr/bin/env python

# fast s3 + glacier backups - Glacier backups
#
# Written by Jason Bishop <jason.bishop@gmail.com>
# Copyright 2014
#     The Board of Trustees of the Leland Stanford Junior University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.



import threading, Queue
import os, io, datetime
import boto
from boto.s3.connection import Location
import boto.s3.lifecycle
from path import path
import sys
import multiprocessing
import hashlib
import pprint
import argparse

from cffi import FFI

ffi = FFI()
ffi.cdef("""
typedef long long time_t;

typedef struct timespec {
    time_t   tv_sec;
    long     tv_nsec;
    ...;
};

typedef struct stat {
    struct timespec st_atim;
    struct timespec st_mtim;
    struct timespec st_ctim;
    ...;
};

int lstat(const char *path, struct stat *buf);
""")

C = ffi.verify()

s3endpoint = 's3-us-west-2.amazonaws.com'
#s3endpoint = 's3-us-west-1.amazonaws.com'
#s3endpoint = 's3.amazonaws.com'


chunk = 10*1024*4096

class managesmallfiles(multiprocessing.Process):
    def __init__(self, num, filequeue, bucketname, *args, **kwargs):
        # job queue
        self.filequeue = filequeue
        self.bucketname = bucketname

        conn = boto.connect_s3(host=s3endpoint, is_secure=True)

        print 'bucket_name', self.bucketname
        self.bucket = conn.lookup(self.bucketname)
        multiprocessing.Process.__init__(self)


    def dos3upload(self, fileobj):
        statoutput = fileobj.stat()

        result = ffi.new("struct stat *")
        p = C.lstat(str(fileobj.realpath()), result)
        mymtime = "{0:d}.{1:09d}".format(result.st_mtim.tv_sec, result.st_mtim.tv_nsec)
        myctime = "{0:d}.{1:09d}".format(result.st_ctim.tv_sec, result.st_ctim.tv_nsec)
        myatime = "{0:d}.{1:09d}".format(result.st_atim.tv_sec, result.st_atim.tv_nsec)

        newobject = self.bucket.new_key('smallfile/' + fileobj.realpath())
        newobject.set_metadata('metadataversion', '1')
        newobject.set_metadata('creator', 'fasts3plusglacier')
        newobject.set_metadata('appversion', '1')
        newobject.set_metadata('ctime', myctime)
        newobject.set_metadata('mtime', mymtime)
        newobject.set_metadata('atime', myatime)
        newobject.set_metadata('ownername', fileobj.owner)
        newobject.set_metadata('mode', oct(statoutput.st_mode & 0777))
        newobject.set_metadata('owneruid', statoutput.st_uid)
        newobject.set_metadata('groupuid', statoutput.st_gid)
        newobject.set_metadata('groupname', 'notimplemented')
        newobject.set_metadata('sha256hashtree', computesha256hashtree(fileobj.realpath()))
        newobject.set_contents_from_filename(fileobj.realpath(), reduced_redundancy=False)

    def run(self):
        while True:
            fileobj = self.filequeue.get()
            if fileobj is None:
                break

            print fileobj.realpath()
            self.dos3upload(fileobj)


class Largefileblockworker(threading.Thread):
    def __init__(self, num, mp_bucket, f, mp_keyname, mp_id, chunkqueue, needmorequeue, timewasted, result, hashes, *args, **kwargs):
        self.mynumber = num
        self.data = bytearray(chunk)
        self.f = f

        conn = boto.connect_s3(host=s3endpoint, is_secure=True)

        bucket = conn.lookup(mp_bucket)
        self.mp = boto.s3.multipart.MultiPartUpload(bucket)
        self.mp.key_name = mp_keyname
        self.mp.id = mp_id
        self.mp.bucket_name = bucket_name
        self.chunkqueue = chunkqueue
        self.needmorequeue = needmorequeue
        self.timewasted = timewasted
        self.result = result
        self.hashes = hashes
        threading.Thread.__init__(self)


    def readdata(self, uploadindex, timeenqueued, (begin, length)):
        metime = datetime.datetime.now()
        differential = (metime-timeenqueued).seconds + (metime-timeenqueued).microseconds/1e6
        self.timewasted.append(differential)
        self.f.seek(begin)
        bytesread = self.f.readinto(self.data)
        if bytesread < chunk:
            self.data = bytearray(self.data[:bytesread])
        return '%d bytes read %d to %d' % (bytesread, begin, bytesread+begin)


    def dos3upload(self, uploadindex, timeenqueued, (begin, mylength)):
        self.mp.upload_part_from_file(io.BytesIO(self.data), uploadindex+1)


    def computehash(self, uploadindex, timeenqueued, (begin, mylength)):
        for index, (start,end) in enumerate([ (i, min(i+1024*1024, len(self.data))) for i in range(0, len(self.data), 1024*1024) ]):
            self.hashes[uploadindex].append(hashlib.sha256(memoryview(self.data[start:end]).tobytes()).digest())


    def run(self):
        while True:
            args = self.chunkqueue.get()
            if args is None:
                self.chunkqueue.task_done()
                break
            msg = self.readdata(*args)
            self.result.append(msg)
            self.needmorequeue.put('msg=[%s], %d is done' % (msg, self.mynumber))
            self.dos3upload(*args)
            self.computehash(*args)
            self.chunkqueue.task_done()

def split_offsets(in_file):
    size = os.path.getsize(in_file)
    return [ (i, min(chunk, size-i)) for i in range(0, size, chunk) ]


class managebigfiles(multiprocessing.Process):
    def __init__(self, num, filequeue, bucketname, *args, **kwargs):
        # job queue
        self.chunkqueue = Queue.Queue()
        self.needmorequeue = Queue.Queue()
        self.filequeue = filequeue
        self.bucketname = bucketname

        self.result = []
        self.timewasted = []

        conn = boto.connect_s3(host=s3endpoint, is_secure=True)

        print 'bucket_name', self.bucketname
        bucket = conn.lookup(self.bucketname)
        multiprocessing.Process.__init__(self)


    def run(self):
        while True:
            fileobj = self.filequeue.get()
            if fileobj is None:
                break
            file = fileobj.realpath()
            mb_size = fileobj.size / 1e6

            print file
            if fileobj.size <= 4*1024*1024:
                statoutput = fileobj.stat()

                result = ffi.new("struct stat *")
                p = C.lstat(str(fileobj.realpath()), result)
                mymtime = "{0:d}.{1:09d}".format(result.st_mtim.tv_sec, result.st_mtim.tv_nsec)
                myctime = "{0:d}.{1:09d}".format(result.st_ctim.tv_sec, result.st_ctim.tv_nsec)
                myatime = "{0:d}.{1:09d}".format(result.st_atim.tv_sec, result.st_atim.tv_nsec)

                newobject = self.bucket.new_key('largefile/' + fileobj.realpath())
                newobject.set_metadata('metadataversion', '1')
                newobject.set_metadata('creator', 'fasts3plusglacier')
                newobject.set_metadata('appversion', '1')
                newobject.set_metadata('ctime', myctime)
                newobject.set_metadata('mtime', mymtime)
                newobject.set_metadata('atime', myatime)
                newobject.set_metadata('ownername', fileobj.owner)
                newobject.set_metadata('mode', oct(statoutput.st_mode & 0777))
                newobject.set_metadata('owneruid', statoutput.st_uid)
                newobject.set_metadata('groupuid', statoutput.st_gid)
                newobject.set_metadata('groupname', 'notimpleemnted')
                newobject.set_metadata('sha256hashtree', computesha256hashtree(fileobj.realpath()))
                newobject.set_contents_from_filename(fileobj.realpath(), reduced_redundancy=False)

            else:
                hashes = [ [ ] for i,(x,y) in enumerate(split_offsets(file)) ]
                print 'lenth of hashes',len(hashes)

                mp = bucket.initiate_multipart_upload('largefile/' + file, reduced_redundancy=False)

                f = io.open(file, mode='rb', buffering=4*1024*1024)
                workers = [ ]
                for i in range(4):
                    w = Largefileblockworker(i, bucket_name, f, mp.key_name, mp.id, self.chunkqueue, self.needmorequeue, self.timewasted, self.result, hashes)
                    w.setDaemon(1)
                    workers.append(w)
                    w.start()

                start = datetime.datetime.now()

                for (i, (begin, length)) in enumerate(split_offsets(file)):
                    self.chunkqueue.put((i, datetime.datetime.now(), (begin, length)))
                    self.needmorequeue.get()
                    self.needmorequeue.task_done()
                for i in range(4):
                    self.chunkqueue.put(None)
                self.chunkqueue.join()
                dataend = datetime.datetime.now()
                print 'join waiting', dataend
                f.close()


                print ''
                print 'firing complete upload'
                completedupload = mp.complete_upload()
                print 'complete upload done'
                for worker in workers:
                    worker.join()
                finalend = datetime.datetime.now()

                print 'completed location',completedupload.location
                print 'completed key_name',completedupload.key_name
                print 'completed version',completedupload.version_id
                print 'completed etag',completedupload.etag

                hashbegin = datetime.datetime.now()

                linearhashes = [ item for sublist in [ [ y for y in x ] for x in hashes ] for item in sublist ]

                while len(linearhashes) > 1:
                    new_hashes = []
                    while True:
                        if len(linearhashes) > 1:
                            first = linearhashes.pop(0)
                            second = linearhashes.pop(0)
                            new_hashes.append(hashlib.sha256(first + second).digest())
                        elif len(linearhashes) == 1:
                            only = linearhashes.pop(0)
                            new_hashes.append(only)
                        else:
                            break
                    linearhashes.extend(new_hashes)


                result = ffi.new("struct stat *")
                p = C.lstat(str(file), result)
                mymtime = "{0:d}.{1:09d}".format(result.st_mtim.tv_sec, result.st_mtim.tv_nsec)
                myctime = "{0:d}.{1:09d}".format(result.st_ctim.tv_sec, result.st_ctim.tv_nsec)
                myatime = "{0:d}.{1:09d}".format(result.st_atim.tv_sec, result.st_atim.tv_nsec)

                print 'hash tree', ''.join(["%02x" % ord(x) for x in linearhashes[0]]).strip()
                hashend = datetime.datetime.now()
                statoutput = fileobj.stat()
                kk = bucket.get_key(completedupload.key_name)
                kk = kk.copy(kk.bucket.name, kk.name, { 'metadataversion': '1', 'creator': 'fasts3plusglacier',
                                                        'appversion': '1',
                                                        'ctime': myctime, 'mtime': mymtime,
                                                        'atime': myatime, 'ownername': fileobj.owner,
                                                        'mode': oct(statoutput.st_mode & 0777), 'owneruid': statoutput.st_uid,
                                                        'groupuid': statoutput.st_gid, 'groupname': 'notimpleemnted',
                                                        'sha256hashtree': ''.join(["%02x" % ord(x) for x in linearhashes[0]]).strip()
                             }, preserve_acl=True)
                copyupend = datetime.datetime.now()

                print 'file portion MB/s', (os.path.getsize(file)/(1024.0*1024.0))/((dataend-start).seconds + (dataend-start).microseconds/1e6)
                print 'final        MB/s', (os.path.getsize(file)/(1024.0*1024.0))/((finalend-start).seconds + (finalend-start).microseconds/1e6)
                print 'total upload time',(finalend-start).seconds + (finalend-start).microseconds/1e6
                print 'time wasted', sum(self.timewasted)
                print 'hash time',(hashend-hashbegin).seconds + (hashend-hashbegin).microseconds/1e6
                print 'copy time',(copyupend-hashbegin).seconds + (copyupend-hashbegin).microseconds/1e6



class awsobjects():
    def __init__(self, bucket_name):
        self.bucketname = bucket_name
        self.amazonfiles = { }
        self.amazonfilescounter = 0

        conn = boto.connect_s3(host=s3endpoint, is_secure=True)

        print 'aws objects connecting to', self.bucketname
        self.bucket = conn.lookup(self.bucketname)

        amazonlistbegintime = datetime.datetime.now()
        awscounter = 0
        print 'getting contents of bucket'
        for key in bucket.list():
            awscounter += 1
            storageclass = key.storage_class
            self.amazonfiles[key.name] = { 'name': key.name, 'storageclass': storageclass,
                                      'mtime': None, 'atime': None,
                                      'ctime': None, 'sha256hashtree': None,
                                      'ongoing_restore': None, 'expiry_date': None, 'key': key
                                      }


    def getdict(self):
        return self.amazonfiles


    def get(self, objectname, attr):
        if not self.amazonfiles.has_key(objectname):
            print 'aint got it',objectname
            return False
        if self.amazonfiles[objectname]['mtime'] == None:
#            print 'worker working on',self.amazonfiles[objectname]['key']
            key = bucket.get_key(self.amazonfiles[objectname]['key'])
            self.amazonfiles[objectname]['mtime'] = key.get_metadata('mtime')
            self.amazonfiles[objectname]['atime'] = key.get_metadata('atime')
            self.amazonfiles[objectname]['ctime'] = key.get_metadata('ctime')
            self.amazonfiles[objectname]['sha256hashtree'] = key.get_metadata('sha256hashtree')
            self.amazonfiles[objectname]['ongoing_restore'] = key.ongoing_restore
            self.amazonfiles[objectname]['expiry_date'] = key.expiry_date

        return self.amazonfiles[objectname][attr]


def computesha256hashtree(file):
    hashes = [ ]
    f = io.open(file, mode='rb', buffering=4*1024*1024)
    for index, (start,end) in enumerate([ (i, min(i+1024*1024, os.path.getsize(file))) for i in range(0, os.path.getsize(file), 1024*1024) ]):
        hashes.append(hashlib.sha256(f.read(1024*1024)).digest())
    f.close()

    while len(hashes) > 1:
        new_hashes = []
        while True:
            if len(hashes) > 1:
                first = hashes.pop(0)
                second = hashes.pop(0)
                new_hashes.append(hashlib.sha256(first + second).digest())
                #            print ''.join(["%02x" % ord(x) for x in hashlib.sha256(first + second).digest()]).strip()
            elif len(hashes) == 1:
                only = hashes.pop(0)
                new_hashes.append(only)
            else:
                break
        hashes.extend(new_hashes)
    if not hashes:
        return ''.join(["%02x" % ord(x) for x in hashlib.sha256('').digest() ])
    return ''.join(["%02x" % ord(x) for x in hashes[0]]).strip()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Research Computing S3/Glacier backups', fromfile_prefix_chars="@")
    parser.add_argument('directories', help='dirs to scan', action='append', nargs='*')
    parser.add_argument('--restore', help='restore given directory from Glacier', default=False, action='store_true')
    parser.add_argument('--backup', help='backup files in listed directories to S3/Glacier', default=False, action='store_true')
    parser.add_argument('--show-bucket-policy', help='show bucket policy for moving files to glacier', default=False, action='store_true')
    parser.add_argument('--check-sha256', help='when comparing, use sha hash instead of mtime', default=False, action='store_true')
    parser.add_argument('--bucketname', help='which bucket to use', required=True, action='store')
    parser.add_argument('--verbose', help='verbose output', default=False, action='store_true')
    args = parser.parse_args()
    print (args)

    conn = boto.connect_s3(host=s3endpoint, is_secure=True)

    bucket_name = args.bucketname
    bucket = conn.get_bucket(bucket_name)

    try:
        current = bucket.get_lifecycle_config()
        if args.show_bucket_policy:
            print current
            print current[0].transition
            if current:
                rules = (r for r in current if type(r) is boto.s3.lifecycle.Rule)
                for rule in rules:
                    print 'rule id',rule.id
                    print 'rule prefix',rule.prefix
                    print 'rule status',rule.status

    #            print 'killing lifecycle config for bucket'
    #            bucket.delete_lifecycle_configuration()

    except boto.exception.S3ResponseError, foo:
        if foo.status == 404 and foo.error_code == u'NoSuchLifecycleConfiguration':
            print 'no lifecycle config yet.  creating one.'
            to_glacier = boto.s3.lifecycle.Transition(days=0, storage_class='GLACIER')
            rule = boto.s3.lifecycle.Rule('bigfilerule', 'largefile/', 'Enabled', transition=to_glacier)
            lifecycle = boto.s3.lifecycle.Lifecycle()
            lifecycle.append(rule)
            bucket.configure_lifecycle(lifecycle)
        else:
            raise foo


    awsobjectlookup = awsobjects(bucket_name)
    amazonfiles = awsobjectlookup.getdict()

    def listwithprefix(pre, seenthese):
        for key in bucket.list(prefix=pre, delimiter='/'):
#            print key.name
#            print key.name.count('/')
            if key.name.count('/') < 4 and key.name not in seenthese and key.name.endswith('/'):
                seenthese.append(key.name)
                listwithprefix(key.name, seenthese)
        return seenthese

    listofstuff = listwithprefix('largefile/', [])
    for i in listofstuff:
        print 'checking',i,i.rsplit('/',2)[0]
        if i.rsplit('/', 2)[0]+'/' in listofstuff:
            listofstuff.remove(i.rsplit('/', 2)[0]+'/')

    for i in listofstuff:
        print i
    print ''
    print ''


    if args.restore:
        numberofrestoreswearewaitingfor = 0
        numbreofnewrestoresfired = 0
        dirtorestore = args.directories[0][0]
        if not dirtorestore.endswith('/'):
            dirtorestore += '/'
        print 'searching',dirtorestore,'for available files'
        print ''
        for i in amazonfiles.keys():
            if i.startswith('largefile' + dirtorestore):
                print 'found largefile',amazonfiles[i]['name']
                print 'storage class',amazonfiles[i]['storageclass']
                print 'ongoing restore',awsobjectlookup.get(i, 'ongoing_restore')
                print 'expiry date',awsobjectlookup.get(i, 'expiry_date')
                if not os.path.exists('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,'')):
                    if awsobjectlookup.get(i, 'storageclass') != 'GLACIER':
                        # for some reason this one isn't in glacier
                        # restore anyway
                        if not os.path.exists('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('largefile' + dirtorestore,''))):
                            os.makedirs('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('largefile' + dirtorestore,'')))

                        print 'restoring file'
                        amazonfiles[i]['key'].get_contents_to_filename('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,''))
                        os.utime('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,''), (float(awsobjectlookup.get(i, 'atime')), float(awsobjectlookup.get(i, 'mtime'))))
                    else:
                        if awsobjectlookup.get(i, 'storageclass') == 'GLACIER' and awsobjectlookup.get(i, 'ongoing_restore') == True:
                            print 'restore is going on'
                            numberofrestoreswearewaitingfor += 1
                        else:
                            if awsobjectlookup.get(i, 'storageclass') == 'GLACIER' and awsobjectlookup.get(i, 'ongoing_restore') == False and awsobjectlookup.get(i, 'expiry_date') != None:
                                print 'restore is done and available until',awsobjectlookup.get(i, 'expiry_date')

                                if not os.path.exists('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('largefile' + dirtorestore,''))):
                                    os.makedirs('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('largefile' + dirtorestore,'')))

                                if not os.path.exists('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,'')):
                                    print 'glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,''),'does not exist, restoring',
                                    amazonfiles[i]['key'].get_contents_to_filename('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,''))
                                    print 'setting atime/mtime'
                                    os.utime('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('largefile' + dirtorestore,''), (float(awsobjectlookup.get(i, 'atime')), float(awsobjectlookup.get(i, 'mtime'))))
                            else:
                                print 'firing the restore for this GLACIER file (please allow 3-5hrs)'
                                restoreit = bucket.get_key(amazonfiles[i]['name'])
                                restoreit.restore(days=1)
                                numbreofnewrestoresfired += 1
                else:
                    print 'file has already been downloaded to glacierrestore directory'
                print ''

            if i.startswith('smallfile' + dirtorestore):
                print 'found smallfile',amazonfiles[i]['name']
                print 'storage class',amazonfiles[i]['storageclass']

                if not os.path.exists('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('smallfile' + dirtorestore,''))):
                    os.makedirs('glacierrestore' + os.sep + os.path.dirname(amazonfiles[i]['name'].replace('smallfile' + dirtorestore,'')))

                if not os.path.exists('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('smallfile' + dirtorestore,'')):
                    print 'glacierrestore' + os.sep + amazonfiles[i]['name'].replace('smallfile' + dirtorestore,''),'does not exist, restoring'
                    amazonfiles[i]['key'].get_contents_to_filename('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('smallfile' + dirtorestore,''))
                    os.utime('glacierrestore' + os.sep + amazonfiles[i]['name'].replace('smallfile' + dirtorestore,''), (float(awsobjectlookup.get(i, 'atime')), float(awsobjectlookup.get(i, 'mtime'))))
                print ''

        print ''
        print 'number of files currently being restored from glacier to S3:',numberofrestoreswearewaitingfor
        print 'number of new file restores initiated:',numbreofnewrestoresfired
        print ''

    if not args.restore:
        smallfilesqueues = [ ]
        bigfilesqueues = [ ]

        file_count = 0
        bigfile_upload_count = 0
        smallfile_upload_count = 0
        dir_count = 0
        total = 0

        for dir in args.directories[0]:
            smallfilesqueue = multiprocessing.Queue()
            smallfilesqueues.append(smallfilesqueue)
            bigfilesqueue = multiprocessing.Queue()
            bigfilesqueues.append(bigfilesqueue)

            print 'scanning dir',dir
            d = path(dir)
            for i in d.walk():
                if i.isfile():
                    file_count += 1
                    if i.size > 262144:
                        if amazonfiles.has_key('largefile' + i.realpath()):
                            print i.realpath()
                            awsfile = amazonfiles['largefile' + i.realpath()]
                            if args.check_sha256:
                                mysha = computesha256hashtree(i.realpath())
                                if mysha == awsobjectlookup.get('largefile' + i.realpath(), 'sha256hashtree'):
                                    print 'already uploaded, shas match'
                                else:
                                    print 'already uploaded, but shas do not match'
                                    if args.backup:
                                        bigfilesqueue.put(i)
                                        bigfile_upload_count += 1
                            else:
                                result = ffi.new("struct stat *")
                                p = C.lstat(str(i.realpath()), result)
                                mymtime = "{0:d}.{1:09d}".format(result.st_mtim.tv_sec, result.st_mtim.tv_nsec)

                                if awsobjectlookup.get('largefile' + i.realpath(), 'mtime') == mymtime:
                                    print 'already uploaded, mtimes match (%s)' % awsobjectlookup.get('largefile' + i.realpath(), 'mtime')
                                else:
                                    print 'already uploaded, mtimes do not match'
                                    if args.backup:
                                        bigfilesqueue.put(i)
                                        bigfile_upload_count += 1
                        else:
                            if args.backup:
                                bigfilesqueue.put(i)
                                bigfile_upload_count += 1
                            else:
                                if args.verbose:
                                    print i.realpath(),'compare only, not uploading'
                    else:
                        if amazonfiles.has_key('smallfile' + i.realpath()):
                            print i.realpath()
                            awsfile = amazonfiles['smallfile' + i.realpath()]
                            if args.check_sha256:
                                mysha = computesha256hashtree(i.realpath())
                                if mysha == awsobjectlookup.get('smallfile' + i.realpath(), 'sha256hashtree'):
                                    print 'already uploaded, shas match'
                                else:
                                    print 'already uploaded, but shas do not match'
                                    if args.backup:
                                        smallfilesqueue.put(i)
                                        smallfile_upload_count += 1
                            else:
                                result = ffi.new("struct stat *")
                                p = C.lstat(str(i.realpath()), result)
                                mymtime = "{0:d}.{1:09d}".format(result.st_mtim.tv_sec, result.st_mtim.tv_nsec)

                                if awsobjectlookup.get('smallfile' + i.realpath(), 'mtime') == mymtime:
                                    print 'already uploaded, mtimes match (%s)' % awsobjectlookup.get('smallfile' + i.realpath(), 'mtime')
                                else:
                                    print 'already uploaded, mtimes do not match'
                                    if args.backup:
                                        smallfilesqueue.put(i)
                                        smallfile_upload_count += 1
                        else:
                            if args.backup:
                                smallfilesqueue.put(i)
                                smallfile_upload_count += 1
                            else:
                                if args.verbose:
                                    print i.realpath(),'compare only, not uploading'
                elif i.isdir():
                    dir_count += 1
                else:
                    pass
                total += 1
            print ''

        print "Total number of directories scanned == {0}".format(dir_count)
        print "Total number of files scanned == {0}".format(file_count)
        print "Total number of big files to upload == {0}".format(bigfile_upload_count)
        print "Total number of small files to upload == {0}".format(smallfile_upload_count)
    #    print 'amazon bucket list',awscounter,'in',(amazonlistendtime-amazonlistbegintime).seconds + (amazonlistendtime-amazonlistbegintime).microseconds/1e6,'seconds'
        print ''

        if bigfile_upload_count > 0:
            bigfilebegintime = datetime.datetime.now()
            bigfileworkers = [ ]
            for q in bigfilesqueues:
                print 'kicking off 2 workers for file queue'
                for i in range(4):
                    w = managebigfiles(i, q, bucket_name)
                    bigfileworkers.append(w)
                    w.start()

                for i in range(4):
                    q.put(None)

            print ''
            for worker in bigfileworkers:
                worker.join()
            bigfileendtime = datetime.datetime.now()
            print 'largefileupload took',(bigfileendtime-bigfilebegintime).seconds + (bigfileendtime-bigfilebegintime).microseconds/1e6



        if smallfile_upload_count > 0:
            smallfilebegintime = datetime.datetime.now()
            smallfileworkers = [ ]
            for q in smallfilesqueues:
                print 'kicking off 2 workers for file queue'
                for i in range(40):
                    w = managesmallfiles(i, q, bucket_name)
                    smallfileworkers.append(w)
                    w.start()

                for i in range(40):
                    q.put(None)

            print ''
            for worker in smallfileworkers:
                worker.join()
            smallfileendtime = datetime.datetime.now()
            print 'smallfileupload took',(smallfileendtime-smallfilebegintime).seconds + (smallfileendtime-smallfilebegintime).microseconds/1e6
