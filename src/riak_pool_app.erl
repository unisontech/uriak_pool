-module(riak_pool_app).

-behaviour(application).
-export([start/2,stop/1]).

-spec start(any(), any()) -> any().
start(_Type, _StartArgs) ->
    riak_pool_balancer:start_link(),
    riak_pool_sup:start_link().

-spec stop(any()) -> any().
stop(_State) ->
    ok.
