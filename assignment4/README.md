# Distributed File System

This project is a distributed file system built using RAFT as the underneath consensus protocol. The file system provides 4 operation i.e to read, to write, to update (using compare and swap) and delete files. Clients can connect to any of the live cluster nodes.

The configuration is mentioned in the config file. The read in this filesystem is not replicated and hence may return stale answers, how ever the system is eventually consistent.

### Running the System

```
go get github.com/aditya-nambiar/cs733/assignment4
go test github.com/aditya-nambiar/cs733/assignment4
go build server.go raft_node.go server_config.go node.go ClientHandler.go
./server <server-id> <false> 
```

Once the entire cluster is up, clients can connect to the server, in case of Read requests the client is directly served from the node it contacts
in other cases it is redirected to the leader.

### Commands
Client can issue the following commands :

##### 1. read
Given a filename, retrieve the corresponding file from the server. Return metadata( version number, num_bytes, expiry time) along with the content of file. If the file is not present on the server, "ERR_FILE_NOT_FOUND" error will be received.

Syntax:
```
	read <file_name>\r\n
```

Response (if successful):

``` CONTENTS <version> <num_bytes> <exptime>\r\n ```<br />
``` <content_bytes>\r\n ```


##### 2. write

Create a file, or update the file’s contents if it already exists. Files are given new version numbers when updated.

Syntax:
```
	write <file_name> <num_bytes> [<exp_time>]\r\n
	<content bytes>\r\n
```

Response:

``` OK <version>\r\n ```

##### 3. compare-and-swap (cas)

This replaces the old file contents with the new content provided the version of file is still same as that in the command. Returns OK and new version of the file if successful, else error in cas and the new version, if unsuccessful.

Syntax:
```
	cas <file_name> <version> <num_bytes> [<exp_time>]\r\n
	<content bytes>\r\n
```

Response(If successful): 

``` OK <version>\r\n ```

##### 4. delete

Delete a file. Returns FILE_NOT_FOUND error if file is not present.

Syntax:
```
	delete <file_name>\r\n
```

Response(If successful):


``` OK\r\n ```


### ERRORS
You may come across the following errors:
(Here is the meaning and the scenario in which they can occur)

``` ERR_CMD_ERR ```:                          Incorrect client command

``` ERR_FILE_NOT_FOUND ```:                   File requested is not found (Either expired or not created) 

``` ERR_VERSION <latest-version> ```:         Error in cas (Version mismatch)

``` ERR_INTERNAL ```:                         Internal server error 

``` ERR_REDIRECT <leader-ip:leader-port> ```: Redirect the client to the leader node


### Contact

Aditya Nambiar <br />
Email: aditya.nambiar007@gmail.com