# Storm Distributed Cache API

The distributed cache feature in storm is used to efficiently distribute files
(or blobs, which is the equivalent terminology for a file in the distributed
cache and is used interchangeably in this document) that are large and can
change during the lifetime of a topology, such as geo-location data,
dictionaries, etc. Typical use cases include phrase recognition, entity
extraction, document classification, URL re-writing, location/address detection
and so forth. Such files may be several KB to several GB in size. For small
datasets that don't need dynamic updates, including them in the topology jar
could be fine. But for large files, the startup times could become very large.
In these cases, the distributed cache feature can provide fast topology startup,
especially if the files were previously downloaded for the same submitter and
are still in the cache. This is useful with frequent deployments, sometime a few
a day with updated jars, because the large cached files will remain available
without changes. The large cached blobs that do not change frequently will
remain available in the distributed cache.

At the starting time of a topology, the user specifies the set of files the
topology needs. Once a topology is running, the user at any time can request for
any file in the distributed cache to be updated with a newer version. The
updating of blobs happens in an eventual consistency model. If the topology
needs to know what version of a file it has access to, it is the responsibility
of the user to find this information out. The files are stored in a cache with
Least-Recently Used (LRU) eviction policy, where the supervisor decides which
cached files are no longer needed and can delete them to free disk space. The
blobs can be compressed, and the user can request the blobs to be uncompressed
before it accesses them.

## Using the Distributed Cache API, Command Line Interface (CLI)

### Creating blobs 

To use the distributed cache feature, the user first has to "introduce" files
that need to be cached and bind them to key strings. To achieve this, the user
uses the "blobstore create" command of the storm executable, as follows:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore create [-f|--file FILE] [-a|--acl ACL1,ACL2,...] [keyname]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The contents come from a FILE, if provided by -f or --file option, otherwise
from STDIN.  
The ACLs, which can also be a comma separated list of many ACLs, is of the
following format:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
> [u|o]:[username]:[r-|w-|a-|_]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

where:  

* u = user  
* o = other  
* username = user for this particular ACL  
* r = read access  
* w = write access  
* a = admin access  
* _ = ignored  

###### Example:  


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore create --file README.txt --acl o::rwa key1  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the above example, the *README.txt* file is added to the distributed cache.
It can be accessed using the key string "*key1*" for any topology that needs
it. The file is set to have read/write/admin access for others, a.k.a world
everything.

###### Example:  

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore create mytopo:data.tgz -f data.tgz -a u:alice:rwa,u:bob:rw,o::r  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above example createss a mytopo:data.tgz key using the data stored in
data.tgz.  User alice would have full access, bob would have read/write access
and everyone else would have read access.

### Making dist. cache files accessible to topologies

Once a blob is created, we can use it for topologies. This is generally achieved
by including the key string among the configurations of a topology, with the
following format. A shortcut is to add the configuration item on the command
line when starting a topology by using the **-c** command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-c topology.blobstore.map='{"[KEY]":{localname:"[VALUE]", uncompress:[true|false]}}'
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The cache file would then be accessible to the topology as a local file with the
name [VALUE].  
The localname parameter is optional, if omitted the local cached file will have
the same name as [KEY].  
The uncompress parameter is optional, if omitted the local cached file will not
be uncompressed.  Note that the key string needs to have the appropriate
file-name-like format and extension, so it can be uncompressed correctly.

###### Example:  

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm jar /home/y/lib/storm-starter/current/storm-starter-jar-with-dependencies.jar storm.starter.clj.word_count test_topo -c topology.blobstore.map='{"key1":{localname:"blob_file", uncompress:false},"key2":{}}'
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the above example, we start the *word_count* topology (stored in the
*storm-starter-jar-with-dependencies.jar* file), and ask it to have access
to the cached file stored with key string = *key1*. This file would then be
accessible to the topology as a local file called *blob_file*, and the
supervisor will not try to uncompress the file. Note that in our example, the
file's content originally came from *README.txt*. We also ask for the file
stored with the key string = *key2* to be accessible to the topology. Since
both the optional parameters are omitted, this file will get the local name =
*key2*, and will not be uncompressed.

### Updating a cached file

It is possible for the cached files to be updated while topologies are running.
The update happens in an eventual consistency model, where the supervisors poll
Nimbus every 30 seconds, and update their local copies. In the current version,
it is the user's responsibility to check whether a new file is available.

To update a cached file, use the following command. Contents come from a FILE or
STDIN. Write access is required to be able to update a cached file.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore update [-f|--file NEW_FILE] [KEYSTRING]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

###### Example:  

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore update -f updates.txt key1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the above example, the topologies will be presented with the contents of the
file *updates.txt* instead of *README.txt* (from the previous example), even
though their access by the topology is still through a file called
*blob_file*.

### Removing a cached file

To remove a file from the distributed cache, use the following command. Removing
a file requires write access.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore delete [KEYSTRING]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Listing Blobs currently in the distributed cache blob store

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore list [KEY...]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

lists blobs currently in the blob store

### Reading the contents of a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm blobstore cat [-f|--file FILE] KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

read a blob and then either write it to a file, or STDOUT. Reading a blob
requires read access.

### Setting the access control for a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
set-acl [-s ACL] KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ACL is in the form [uo]:[username]:[r-][w-][a-] can be comma  separated list
(requires admin access).

### Command line help

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
storm help blobstore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Using the Distributed Cache API from Java

We start by getting a ClientBlobStore object by calling this function:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Config theconf = new Config();
theconf.putAll(Utils.readStormConfig());
ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The required Utils package can by imported by:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import backtype.storm.utils.Utils;
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ClientBlobStore and other blob-related classes can be imported by:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.blobstore.AtomicOutputStream;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.blobstore.BlobStoreAclHandler;
import backtype.storm.generated.*;
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Creating ACLs to be used for blobs

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
String stringBlobACL = "u:username:rwa";
AccessControl blobACL = BlobStoreAclHandler.parseAccessControl(stringBlobACL);
List<AccessControl> acls = new LinkedList<AccessControl>();
acls.add(blobACL); // more ACLs can be added here
SettableBlobMeta settableBlobMeta = new SettableBlobMeta(acls);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The settableBlobMeta object is what we need to create a blob in the next step. 

### Creating a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
AtomicOutputStream blobStream = clientBlobStore.createBlob("some_key", settableBlobMeta);
blobStream.write("Some String or input data".getBytes());
blobStream.close();
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that the settableBlobMeta object here comes from the last step, creating ACLs.
It is recommended that for very large files, the user writes the bytes in smaller chunks (for example 64 KB, up to 1 MB chunks).

### Updating a blob

Similar to creating a blob, but we get the AtomicOutputStream in a different way:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
String blobKey = "some_key";
AtomicOutputStream blobStream = clientBlobStore.updateBlob(blobKey);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass a byte stream to the returned AtomicOutputStream as before. 

### Updating the ACLs of a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
String blobKey = "some_key";
AccessControl updateAcl = BlobStoreAclHandler.parseAccessControl("u:USER:--a");
List<AccessControl> updateAcls = new LinkedList<AccessControl>();
updateAcls.add(updateAcl);
SettableBlobMeta modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);

//Now set write only
updateAcl = BlobStoreAclHandler.parseAccessControl("u:USER:-w-");
updateAcls = new LinkedList<AccessControl>();
updateAcls.add(updateAcl);
modifiedSettableBlobMeta = new SettableBlobMeta(updateAcls);
clientBlobStore.setBlobMeta(blobKey, modifiedSettableBlobMeta);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Reading a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
String blobKey = "some_key";
InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(blobKey);
BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
String blobContents =  r.readLine();
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Deleting a blob

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
String blobKey = "some_key";
clientBlobStore.deleteBlob(blobKey);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Getting a list of blob keys already in the blobstore

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Iterator <String> stringIterator = clientBlobStore.listKeys();
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

