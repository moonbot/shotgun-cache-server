
v0.0.1 - Alpha

# Shotgun Cache

This python module provides the tools required to keep an Elasticsearch database up to date with a remote Shotgun database.  
These tools only focuses on maintaining the database.  
To interact and retrieve data in your scripts and tools, look at [shotgunCacheProxy](http://google.com).

## System Requirements
The cache server can be run on any machine that has Python installed and has network access to your Shotgun server.

You will need to have [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) installed and running.
This can be as simple as downloading, unzipping, and running
```
$ cd into/elasticsearch/folder
$ ./bin/elasticsearch
```

In addition, I recommend installing [Kibana](https://www.elastic.co/downloads/kibana) to get an awesome tool to visualize your cache database and stats.
Again the process can be as simple as downloadin, unzipping, and running
```
$ cd into/kibana/folder
$ ./bin/kibana
```

### Required Python Modules
- [yaml](http://pyyaml.org/)
- [ZeroMQ](http://zeromq.org/bindings:python)
- [Shotgun Python API](https://github.com/shotgunsoftware/python-api) v3.0+


## Configuration

There are two main locations for configuring `shotgunCache`.
	- `config.yaml` file.
	- `entityConfig` folder.

### config.yaml
This is the main configuration for the shotgun cache.
Check the file for details about each of the options.

### entityConfig folder
This folder contains all the settings for caching each shotgun entity type.
The best way to create these is to generate them.

To generate the entity cache configs, use the follow command template.
Entity types should be separate by spaces.
List as many types as you want.
```
$ python shotgunCache generateEntityConfigs Asset Shot
```

## Starting the server

Once you've configured your cache server, you can run it using:
```
$ shotgunCache run
```

It will then startup, perform a full download of all your entity data, and then continually monitor shotgun for changes.  
You only need to run the cache server on a single machine.


## Rebuilding an entity type on-demand

If needed, you can perform a rebuild of certain entity types while the server is still running.
To do this just run this command in a separate process:

```
$ shotgunCache triggerRebuild -h localhost Asset Shot
```