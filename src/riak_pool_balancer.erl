-module(riak_pool_balancer).

-behaviour(gen_server).

%% API
-export([
         start_link/1,
         name/1
        ]).

-export([
         get_pool/0, get_pool/1,
         register_pool/2,
         unregister_pool/2,
         pools_list/1
        ]).

%% gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(POOL_TIMEOUT, 1000).

-record(state, {
          round_robin_pools = [],
          pools_table
         }).

%%%===================================================================
%%% API
%%%===================================================================

name(ClusterName) ->
    riak_pool_misc:gen_name(?MODULE, ClusterName).

start_link(ClusterName) ->
    gen_server:start_link({local, name(ClusterName)}, ?MODULE,
                          [ClusterName], []).

get_pool()->
    case application:get_env(riak_cluster) of
        {ok, ClusterName} ->
            get_pool(ClusterName);
        undefined ->
            case application:get_env(riak_pool, default_cluster) of
                {ok, ClusterName} ->
                    get_pool(ClusterName);
                undefined ->
                    {error, riak_pool_no_cluster_configured}
            end
    end.

get_pool(ClusterName)->
    case gen_server:call(name(ClusterName), get_pool, ?POOL_TIMEOUT) of
        {pools_list, Pool} -> Pool;
        Error              -> Error
    end.

register_pool(ClusterName, PoolName) ->
    ets:insert(name(ClusterName), {pools_list, PoolName}),
    ok.

unregister_pool(ClusterName, PoolName) ->
    ets:delete_object(name(ClusterName), {pools_list, PoolName}),
    ok.

pools_list(ClusterName) ->
    ets:lookup(name(ClusterName), pools_list).
    
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ClusterName]) ->
    PoolsTable = name(ClusterName),
    ets:new(PoolsTable, [bag, named_table, public]),
    {ok, #state{pools_table = PoolsTable}}.

handle_call(get_pool, _From, State =
                #state{
                   round_robin_pools = RoRoPo,
                   pools_table       = PoolsTable
                  }) ->
    case RoRoPo of
        [] ->
            case ets:lookup(PoolsTable, pools_list) of
                [] ->
                    NewState = State#state{round_robin_pools = []},
                    {reply, {error, no_pools_found}, NewState};
                [Pool | NewRoRoPo] ->
                    {reply, Pool, State#state{round_robin_pools = NewRoRoPo}}
            end;
        [Pool | NewRoRoPo] ->
            {reply, Pool, State#state{round_robin_pools = NewRoRoPo}}
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
