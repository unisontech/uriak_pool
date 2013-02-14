Main features
=============

 * Graceful handling of riak nodes starts and shutdowns
 * simultaneous work with different riak clusters
 * **parse_transform** generation for interfaces from the client code
 * simple configuring using erlang configuration files
 * reconfiguration in runtime
 * tested in production at Unison Technologies

Getting started
===============

Building
--------

You can build this software with **rebar** tool:
```shell
rebar get-deps && rebar compile
```

Usage as rebar dependency:

```erlang
{riak_pool, ".*", {git, "git@github.com:unisontech/uriak_pool.git", "master"}}
```

**Important note:** by default, all dependencies are fetched
from github.com/unisontech repositories.
You can change this setup in _rebar.config_ file.
But if you do so (e.g. in order to switch to the newest version of riakc),
something might not work properly.

Configuration
-------------

* You can configure **riak_pool** with a configuration file of your release.

See configuration format description and example [here](etc/app.config)

* You also can reconfigure **riak_pool** in runtime with:

```erlang

riak_pool_clusters_sup:start_cluster/3
riak_pool_clusters_sup:stop_cluster/1

riak_pool_cluster_sup:start_pool/3
riak_pool_cluster_sup:stop_pool/2
```

Usage
-----

**riak_pool** has two different external interfaces.

Both of them are generated with parse_transformation from original
**riak_pb_socket** client code.

parse_transformed modules act as a proxy to the client
module, so you can use original **riakc** documentation for API description.
It makes it possible to use the newest riak API without **riak_pool**
code modification.

* **riak_pool** interface is designed to work with connection
explicitly:

```erlang
take(Worker, Bucket, Key) ->
    {ok, Obj} = riak_pool:get(Worker, Bucket, Key),
    ok = riak_pool:delete_obj(Worker, Obj),
    Obj.

riak_pool:with_worker(fun ?MODULE:take/3, [<<"bucket">>, <<"key">>]).
riak_pool:with_worker(?MODULE, take, [<<"bucket">>, <<"key">>]).
riak_pool:with_worker(fun(Worker) -> take(Worker, <<"bucket">>, <<"key">>) end).
```

* **riak_pool_auto** interface doesn't require connection management from the user.

It may look like a more convenient way but in some cases it's less efficient.
Each call requires check-in and check-out pool operation.
If you're dealing with a lot of operations, it's better to use
**riak_pool** interface.

```erlang
riak_pool_auto:get(<<"bucket">>, <<"key">>).
```
