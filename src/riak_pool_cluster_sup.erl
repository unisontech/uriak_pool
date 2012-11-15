-module(riak_pool_cluster_sup).

-behaviour(supervisor).

%% API
-export([
         start_link/1,
         name/1,
         stop_pool/2,
         start_pool/3
        ]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

name({ClusterName, _ClusterOpts, _Pools}) -> name(ClusterName);
name(ClusterName) ->
    riak_pool_misc:gen_name(?MODULE, ClusterName).    

start_link(Cluster) ->
    supervisor:start_link({local, name(Cluster)}, ?MODULE, Cluster).

start_pool(ClusterName, ClusterOpts, PoolOpts) ->
    ChildSpec = child_spec_for_pool(ClusterName, ClusterOpts, PoolOpts),
    supervisor:start_child(name(ClusterName), ChildSpec).

stop_pool(ClusterName, PoolName) ->
    ChildId = riak_pool_pool_sup:name(ClusterName, PoolName),
    supervisor:terminate_child(name(ClusterName), ChildId),
    supervisor:delete_child(name(ClusterName), ChildId),
    riak_pool_balancer:unregister_pool(ClusterName, PoolName).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init({ClusterName, ClusterOpts, Pools}) ->
    RestartStrategy = one_for_all,
    MaxRestarts     = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    PoolBalancer = {
      riak_pool_balancer:name(ClusterName),
      {riak_pool_balancer, start_link, [ClusterName]},
      permanent, 2000, worker, [riak_pool_balancer]
     },

    PoolsSups =
        [child_spec_for_pool(ClusterName, ClusterOpts, Pool) || Pool  <- Pools],
    
    {ok, {SupFlags, [PoolBalancer] ++ PoolsSups}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

child_spec_for_pool(ClusterName, ClusterOpts, Pool) ->
    {
      riak_pool_pool_sup:name(ClusterName, Pool),
      {riak_pool_pool_sup, start_link, [ClusterName, ClusterOpts, Pool]},
      permanent, 2000, supervisor, [riak_pool_pool_sup]
    }.
    
