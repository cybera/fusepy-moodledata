# Operational Tooling
## tool_sync_dir_to_swift.py
This works, but it would be nice to add some support for command line arguments as opposed to having to
change the code to change the upload directory.

This is a very time consuming script and should probably only be run as a last resort or in cases that
it does not matter that it will take a lot of time and generate a lot of system load.

It would be nice to be able to upload only files specified in some "manifest" file. This would work
very will with the tool that will verify file integrety.

## tool_verify file integrety
This would generate a list of files that are not in sync with Swift.

The idea here is to check any files that are in the local cache and make sure that they exist in Swift
and also have the same MD5 hash. There is a sublety here in that it is possible that a file is in the
process of uploading, so when reporting that it is not in sync its very possible that it will be very
soon.

One way to make sure this process is as efficient as possible is to create a file containing file names
and md5 hashes, and last modified timestamps of files we have already looked at. This avoids the need to
regenerate the hashes every time this is run.

We can further improve the efficiency of this by keeping track of what files we have verified in Swift
and avoid re-checking them unless specifically told otherwise via command line argument. The idea being
that if this script is efficient enough we can run it very frequently and then do a full check maybe
once a day (when additional system load is unlikely to have any impact in production)

To keep things as responsive as possible it may be worth doing this in a multithreaded way.

### output
It would be nice to output a list of files out of sync as a JSON object (i.e. easy to deal with programatically)

## cache pruning script
The idea here is to do some pruning in a almost full cache.

There are a few considerations here though
* We want to be sure not to delete a file that has not yet been uploaded/synced with Swift (maybe a run
  of the "verify file integrety" script will help with this.
* The full cache threshold should probably be passed in as a command line parameter
* we want to make sure not to delete a file that is currently being served to a client
* When pruning, what files should we delete first?
  ** big files first?
  ** old files beyond some threshold?
  ** some combination?
  ** is there some formula we can come up with?


## to do the initial pre-heating of the cache
we can just order the list of files in the file system by last accessed date and load based on that

