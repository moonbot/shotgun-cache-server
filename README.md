
v0.0.1 - Alpha

# Shotgun Cache

This python module provides the tools required to maintain a local caching layer for Shotgun.
This aims to reduce the delay when accessing the data stored in Shotgun.

On average queries to Shotgun take between 100-500 ms, with the local cache this can be reduced to 4-10 ms.

We've utilized Elasticsearch as the database backend due to its speed, and scalability.  
It also doesn't require a full defined schema like SQL.  
With Elasticsearch, we just have to provide hints for how to handle certain data types, and everything else can be handled automatically.

The other great thing about Elasticsearch is its integration with Kibana which can be used to visualize your data very quickly.

## How it works

This caching layer is aimed at provided a partial replica of your Shotgun database.
This allows you to limit the caching to only the entities and fields you need.

On first load for each entity type, we do a batch import.  
Then we maintain the database through a process similar to Shotgun's Event Log Daemon.  
This is accomplished by polling Shotgun for changes at a periodic interval, by default 2 seconds.

## How to use this

This module only provides the tools to maintain the local database.
It doesn't include tools for your scripts to access the database.  
For this, I recommend using our `ShotgunCacheProxy` (Coming Soon) which allows you to use Shotgun's existing API.


## System Requirements
The cache server can be run on any machine that has Python installed and has network access to your Shotgun server.

You will need to have [Elasticsearch](https://www.elastic.co/downloads/elasticsearch) installed and running.
This can be as simple as downloading, unzipping, and running
```
$ cd into/elasticsearch/folder
$ ./bin/elasticsearch
```

In addition, I recommend installing [Kibana](https://www.elastic.co/downloads/kibana) to visualize your cache database and stats.
Again the process can be as simple as downloading, unzipping, and running
```
$ cd into/kibana/folder
$ ./bin/kibana
```

### Required Python Modules
- [Shotgun Python API](https://github.com/shotgunsoftware/python-api) v3.0+
- [ZeroMQ](http://zeromq.org/bindings:python)
- [yaml](http://pyyaml.org/)
- [elasticsearch](https://elasticsearch-py.readthedocs.org/en/master/)


## Configuration

There are two main locations for configuring `shotgunCache`.
- `config.yaml` file.
- `entityConfig` folder.

### config.yaml
This is the main configuration for the shotgun cache.

The most important options to set are your shotgun connection options at the top.  
Fill in your `base_url`, `script_name`, and `api_key`.

Check the file for more details about each option.

### Entity Config Folder
In order to cache entities from Shotgun, the script needs to know which entities and fields you need.
These configurations are stored in a json file per entity inside of the `entity_config_folder`.

Instead of creating these manually, there's a utility that generates them for you.  
Use the follow command as a template.
```
$ python shotgunCache generateEntityConfigs Asset Shot
```

Entity types should be separated by spaces. List as many types as you want.

Once you've generated them, you can open them in your text editor and adjust them.
- Remove any fields from the `fields` key you don't want to cache.  
- Add shotgun filters to the `filters` key to further limit the cache
- You can also use custom elasticsearch mappings here if you really need to


## Starting the server

After you've created your `config.yaml`, generated your entity configs and tweaked them as required, your ready to start the cache.

```
$ python shotgunCache run
```

It will then startup, perform a full download of all your entity data, and then continually monitor shotgun for changes.  
You only need to run the cache server on a single machine.


## Rebuilding an entity type on-demand

If needed, you can perform a rebuild of certain entity types while the server is still running.
To do this just run this command in a separate process:

```
$ python shotgunCache rebuild Asset Shot
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
$ python shotgunCache validateCounts
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
$ python shotgunCache validateData Asset Shot
```

You can also supply different filters or limits.
WARNING: It is advised that you avoid doing complete data validation over large amounts of entities because it causes a heavy load on Shotgun's servers.
Instead restrict your validation to a smaller subset of entities.

```
$ python shotgunCache validateData --filters [['id','greater_than', 1000], ['id','less_than', 4000]] --order [{'field_name':'created_at','direction':'asc'}] --fields ['id','type'] --limit 1000 --page 2
```

## TODO

- Project specific schema for entity config manager?
- Binary support for images, thumbnails, etc...

- Figure out a better way to handle storing of event log entries
	- No easy way to load event log entries for before the cache started due to lack of support
	for filtering based on the `meta` field.
	- We could filter by the `entity` field, but this would only allow for handling entities that aren't currently deleted.
	- It would be great to fit this in the same system as the current entity config for caches, but there would be a lot of work arounds to reproduce the filters and such.
