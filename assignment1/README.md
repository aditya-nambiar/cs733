Assignment 1

This assignment is to build a simple file server, with a simple read/write interface (there’s
no open/delete/rename).

I have uses leveldb to store the files, mutex locks per file for providing concurrency. I have also timed out reads in case needed.

To install level db
go get github.com/syndtr/goleveldb/leveldb


