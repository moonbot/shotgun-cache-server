
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

## Validating the cache

Validating the cache can be performed in two ways
- Count Validation
- Data Validation

NOTE: Any EventLogEntry's recorded during data/count retrieval is factored into the validation.
This fixes the potential loopholes created by the limitation of not being able to load data from a snapshotted point in time.


### Count Validation
This is the quickest form of validation, and the most lightweight.  
It simply retrieves the current entity counts that should be cached from shotgun and compares it to the counts of what's actually stored in the cache db.

Because this is a lightweight and fast check, by default all entity types will be validated in this mode.

```
$ shotgunCache validateCounts
```

You can supply specific entity types to check separated by spaces

```
$ shotgunCache validateCounts Asset Shot
```

#### Data Validation
This is a much heavier process, and can take a lot longer.  
It actually retrieves data from shotgun and compares it to the data stored in the cache db.

By default, this validates the data for all entities, however, the check is limited to the last 500 updated entities.
Unless specified, all cached fields are checked.

```
$ shotgunCache validateData
```

You can supply specific entity types to check separated by spaces

```
$ shotgunCache validateData Asset Shot
```

You can also supply different filters or limits.
WARNING: It is advised that you avoid doing complete data validation over large amounts of entities because it causes a heavy load on Shotgun's servers.
Instead restrict your validation to a smaller subset of entities.

```
$ shotgunCache validateData --filters [['id','greater_than', 1000], ['id','less_than', 4000]] --order [{'field_name':'created_at','direction':'asc'}] --fields ['id','type'] --limit 1000 --page 2
```

#### TODO

- Add automatic EventLogEntry meta field handling
- Implement DB 
- How to handle stat elastic index updates?
	- No backup of data?
- Should I provide default mappings and settings for the stats?
- Need to keep a list of api tokens registered in shotgun


- Counts aren't matching up between imported items and totals
- Having trouble creating a task with an new asset in dev project
- Delete indices for entity types that are no longer cached?

- Create a utility to diff the cache with shotgun
- Create a utility that can signal a rebuild of specific entity types while the controller is running
- Project specific schema for entity config manager?
- Binary support for images, thumbnails, etc...





Figure out how to handle event log enties in the future
No 100% way to do it. Seems really hazy.
# Search history of event logs with
# Can't filter based on the metadata for an event log entry
# The only filters we can use to limit our search of event log entries
# Is the entity type based on the event type
# I could filter by the 'entity' field
# This would limit the results to only entities that were not deleted in shotgun
# So we would not be storing any retirement events for entities
# On revive, we would need to fetch the event log entry history
# I can ignore
# We should store all pertinent event log entries whether they're processed by the cache or not
# Ex:
#   We can ignore processing sg_latest_version changes on Sequences for events on Versions
#   becase we get an event on both the version and the sequence
# Monitor will receive all EventLogEntries that occur for the specified entity types
# However, we will only store events 
