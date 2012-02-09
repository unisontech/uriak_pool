-module(riak_pool_balancer).

-behaviour(gen_server).

%% API
-export([start_link/0, get_pool/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-define(POOL_TIMEOUT, 1000).

-record(state, {pools, current = 1}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_pool()->
    gen_server:call(?SERVER, get_pool, ?POOL_TIMEOUT).
    
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Config} = application:get_env(riak_pool, pools),
    case [Pool || {Pool, _} <- Config] of
        [] ->
            {stop, no_pools_found};
        Pools ->
            {ok, #state{pools=Pools}}
    end.

handle_call(get_pool, _From, State=#state{pools = Pools, current = Current }) ->
    Reply = lists:nth(Current, Pools),
    {reply, Reply, State#state{current = case Q = Current rem length(Pools) of 0->1; _->Q end}};

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
