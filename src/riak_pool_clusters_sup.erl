-module(riak_pool_clusters_sup).

-behaviour(supervisor).

%% API
-export([
         start_link/1,
         start_cluster/3,
         stop_cluster/1
        ]).

%% Supervisor callbacks
-export([
         init/1
        ]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(ClustersConf) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [ClustersConf]).

start_cluster(ClusterName, ClusterOpts, ClusterOpts) ->
    ChildSpec = child_spec_for_cluster({ClusterName, ClusterOpts, ClusterOpts}),
    supervisor:start_child(?SERVER, ChildSpec).

stop_cluster(ClusterName) ->
    ChildId = riak_pool_cluster_sup:name(ClusterName),
    supervisor:terminate_child(?SERVER, ChildId),
    supervisor:delete_child(?SERVER, ChildId).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([ClustersConf]) ->
    RestartStrategy = one_for_one,
    MaxRestarts     = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    ClustersSpecs =
        [child_spec_for_cluster(Cluster) || Cluster <- ClustersConf],

    {ok, {SupFlags, ClustersSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

child_spec_for_cluster(Cluster) ->
    {
      riak_pool_cluster_sup:name(Cluster),
      {riak_pool_cluster_sup, start_link, [Cluster]},
      permanent, 2000, supervisor, [riak_pool_cluster_sup]
    }.
