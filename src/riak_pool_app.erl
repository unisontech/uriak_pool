-module(riak_pool_app).

-behaviour(application).
-export([start/0, start/2,stop/1]).

start() -> application:start(riak_pool).

-spec start(any(), any()) -> any().
start(_Type, _StartArgs) ->
    {ok, ClustersConf} = application:get_env(riak_pool, clusters),
    riak_pool_clusters_sup:start_link(ClustersConf).

-spec stop(any()) -> any().
stop(_State) ->
    ok.
