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

config_to_spec({PoolName, PoolConfig}) ->
  Args = [{name, {local, PoolName}},
          {worker_module, riak_pool_worker} | PoolConfig],
  {PoolName, {poolboy, start_link, [Args]},
   temporary, 5000, worker, [poolboy]}.

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
    
                                       

%% q()->
%%     {ok,
%%      {{one_for_one,10,10},
%%       [{car,{poolboy,start_link,
%%              [[{name,{local,car}},
%%                {worker_module,riak_pool_worker},
%%                {pool_size,10},
%%                {max_overflow,20},
%%                {host,"127.0.0.1"},
%%                {port,8087}]]},
%%         permanent,5000,worker,
%%         [poolboy]},
%%        {prod,{poolboy,start_link,
%%               [[{name,{local,prod}},
%%                 {worker_module,riak_pool_worker},
%%                 {pool_size,5},
%%                 {max_overflow,10},
%%                 {host,"127.0.0.1"},
%%                 {port,8087}]]},
%%         permanent,5000,worker,
%%         [poolboy]}]}}.

