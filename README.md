# ECE-428-MP4

Group Member: yutao2, jiey3

##### Sava 

For workers and masters:

Compile the code: 

```
javac MP4.java
```

Run the code:

```
java MP4
```

The command will show a prompt - "Your command(join, leave, showlist, showid, store, get, put, ls):". Be sure to make vm1 create a group before other vms join the group. After a vm joins the group, you can see the group membership list and its id by "showlist" and "showid". 

Insert or update a file from a local directory into SDFS by "put localfilename sdfsfilename" and fetch a file in SDFS to local directory by "get sdfsfilename localfilename

"ls sdfsfilename" lists all machine (VM) addresses where this file is currently being stored.

"store" lists all files currently being stored at this machine.

For the client:

```
javac Client.java
java Client
```



### How to run PageRank on Sava:

1. Open the masters, workers and the client
2. Upload the input file(graph) on the SDFS
3. Type in the application name and the input file in client: `PageRank pr.txt`

