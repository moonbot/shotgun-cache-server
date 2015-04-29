
# Shotgun Cache

This python module provides the tools required to maintain a local caching layer for Shotgun.  
This aims to reduce the delay when accessing the data stored in Shotgun.

On average queries to Shotgun take between 100-500 ms, with the caching layer this can be reduced to 10-20 ms.

We've utilized [RethinkDB](http://rethinkdb.com/) as the database backend due to its speed, scalability, and query capabilities.  
It's a schema-less database, but still supports joining and merging via querys.

## How it works

This caching layer is aimed at provided a partial replica of your Shotgun database.
This allows you to limit the caching to only the entity types and fields you need.

On first load for each entity type, a batch import is performed loading all existing entities.  
Then, the database is kept in sync through a process similar to Shotgun's Event Log Daemon.  
Shotgun is polled at a periodic interval (default is 2 seconds) for changes using Event Log Entries.
The changes reported in these enties are then applied to the cached data.

## Limitations

- The cache layer is not setup for posting changes in the cache to Shotgun.
  It only receives changes from Shotgun and applies them to the cache.
- When the cache configuration changes, the current items for changed entity types are deleted and reloaded.
- There is a delay associated with the cache based on the `fetch_interval`.  
  By default this is 2 seconds.
  So the data stored in the cache can be up to 2 seconds behind any changes made in Shotgun.
  If your using this cache data in your scripts, you should consider whether this 2 seconds of delay is acceptable.

## How to use

This module only provides the tools to maintain the local database.
It doesn't include tools for your scripts to access the database.  
For this, I recommend using our `ShotgunCacheProxy` (Coming Soon) which allows you to use Shotgun's existing API to communicate to the caching layer.  

You could also you the RethinkDB directly to query data.

## System Requirements
The cache server can be run on any machine that has Python 2.7 installed and has network access to your Shotgun server.

You'll need to install RethinkDB on your server.
More information about the requirements and installation can be found here:  
http://rethinkdb.com/docs/install/

### Required Python Modules
- [Shotgun Python API](https://github.com/shotgunsoftware/python-api) v3.0+
- [ZeroMQ](http://zeromq.org/bindings:python)
- [yaml](http://pyyaml.org/)
- [ruamel.yaml](https://pypi.python.org/pypi/ruamel.yaml/0.6)
- [rethinkdb](http://rethinkdb.com/docs/install-drivers/python/)


## Setup

First thing you need to do is download this repo.
Once downloaded, you can install the script using setup tools.
This should automatically install all python dependencies except the Shotgun Python API which must be installed manually.  
Navigate inside the git repo and run:
```
$ python setup.py install
```

Once installed, you can begin the cache setup process by running
```
$ shotgunCache setup
```

Follow the prompts, providing the required information.
You will need to provide an API key for the script to access Shotgun.
More details about this can be found here:
https://support.shotgunsoftware.com/entries/21193476-How-to-create-and-manage-API-Scripts

After you've run the setup, make any changes required to the generated entity config files.
The entity configs are stored as json files and you can adjust them by:
- Removing any fields from the `fields` key you don't want to cache.  


## Starting the server
Once you've completed the setup process, your ready to start the server

If you installed the config to any location other than the default (~/shotguncache) you'll need to
set the `SHOTGUN_CACHE_CONFIG` environment variable to point to this new path.

```
$ shotgunCache run
```

Once started the server will import all new entities from Shotgun in one batch, then apply the changes received to each entity type from the Event Log Entries.


----------


## TODO

- Project specific schema for entity config manager?
- Binary support for images, thumbnails, etc...
- Implement support for filtering entities that are cached.
	- This is easy to do on initial import
	- However, we need some way to maintain the filters through Event Log updates
- Figure out a way to handle storing of event log entries
	- No easy way to load event log entries for before the cache started due to lack of support
	for filtering based on the `meta` field.
	- We could filter by the `entity` field, but this would only allow for handling entities that aren't currently deleted.
	- It would be great to fit this in the same system as the current entity config for caches, but there would be a lot of work arounds to reproduce the filters and such.
