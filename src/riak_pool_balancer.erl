-module(riak_pool_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, get_pool/0, get_pool/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-define(POOL_TIMEOUT, 1000).

-record(state, {
    orig_clusters      :: dict(),
    curr_clusters      :: dict(),
    default_cluster     :: atom()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_pool()->
    case application:get_env(riak_cluster) of
        {ok, ClusterName} ->
            get_pool(ClusterName);
        undefined ->
            get_pool(undefined)
    end.

get_pool(AppName)->
    gen_server:call(?SERVER, {get_pool, AppName}, ?POOL_TIMEOUT).
    
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Config} = application:get_env(riak_pool, clusters),
    DefaultCluster = case application:get_env(riak_pool, default_cluster) of
        {ok, _DefaultCluster} ->
            _DefaultCluster;
        undefined ->
            undefined
    end,
    
    Pools = lists:foldl(
        fun ({AppName, PoolName}, Acc) ->
            dict:append(AppName, PoolName, Acc)
        end,
        dict:new(),
        [{ClusterName, PoolName} || {ClusterName, ClusterPools} <- Config, {PoolName, _SizeArgs, _PoolConf} <- ClusterPools]
    ),

    {ok, #state{orig_clusters = Pools, curr_clusters = Pools, default_cluster = DefaultCluster}}.

handle_call({get_pool, undefined}, _From, State = #state{default_cluster = undefined}) ->
    {reply, {error, no_pools_found}, State};

handle_call({get_pool, undefined}, From, State = #state{default_cluster = DefaultApp}) ->
    handle_call({get_pool, DefaultApp}, From, State);

handle_call({get_pool, ForApp}, _From, State = #state{orig_clusters = OPools, curr_clusters = CPools}) ->
    case dict:find(ForApp, CPools) of
        {ok, []} ->
            [P | Ps] = dict:fetch(ForApp, OPools),
            NewCPools = dict:store(ForApp, Ps, CPools),
            {reply, P, State#state{curr_clusters = NewCPools}};
        {ok, [P | Ps]} ->
            NewCPools = dict:store(ForApp, Ps, CPools),
            {reply, P, State#state{curr_clusters = NewCPools}};
        error ->
            {reply, {error, no_pools_found}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
