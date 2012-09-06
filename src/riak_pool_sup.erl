-module(riak_pool_sup).

-behaviour(supervisor).

-export([start_link/0, restart/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, Config} = application:get_env(riak_pool, clusters),
  Processes = generate_workers(Config),
  Strategy = {one_for_one, 10, 10},
  {ok,
   {Strategy, lists:flatten(Processes)}}.

generate_workers(AggregateConfig) ->
    [config_to_spec(PoolConf) || {_ClusterName, ClusterPools} <- AggregateConfig, PoolConf <- ClusterPools].

config_to_spec({PoolName, SizeArgs, WorkerArgs}) ->
    PoolArgs = [
        {name, {local, PoolName}}, {worker_module, riak_pool_worker} | SizeArgs
    ],
    poolboy:child_spec(PoolName, PoolArgs, WorkerArgs).

restart() ->
    {ok, Config} = application:get_env(riak_pool, clusters),
    CurNames = supervisor:which_children(?MODULE),
    RestartChildSpecs =
        lists:foldl(
          fun({WName,_,_,_,_,_} = Spec, Acc) ->
                  case lists:keyfind(WName, 1, CurNames) of
                      false -> [Spec | Acc];
                      _     -> Acc
                  end
          end, [], generate_workers(Config)),
    [supervisor:start_child(?MODULE, Child) || Child <- RestartChildSpecs].
