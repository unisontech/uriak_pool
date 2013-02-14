%%% Copyright 2012-2013 Unison Technologies, Inc.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

-module(riak_pool_keep_alive).

-behaviour(gen_fsm).

%% API
-export([start_link/4]).

%% gen_fsm callbacks
-export([
         init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

%% states
-export([
         connecting/2,
         connected/2
        ]).

-record(rec_timer, {
          value     = 200   :: non_neg_integer(),
          factor    = 2     :: integer(),
          incr      = 0     :: integer(),
          max_value = 15000 :: non_neg_integer()
         }).

-record(state, {
          ping_process         = undefined    :: pid(),
          ping_request_timeout = 500          :: integer(),
          rec_timer            = #rec_timer{} :: #rec_timer{},
          raw_rec_timer        = #rec_timer{} :: #rec_timer{},
          cluster_name         = undefined    :: atom(),
          pool_name            = undefined    :: atom()
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ClusterName, ClusterOpts, PoolName, PingProcessArgs) ->
    gen_fsm:start_link(?MODULE, [ClusterName, ClusterOpts,
                                 PoolName, PingProcessArgs], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([ClusterName, ClusterOpts, PoolName, PingProcessArgs]) ->
    riak_pool_balancer:unregister_pool(ClusterName, PoolName),

    {ok, PingProcess} = apply(riakc_pb_socket, start_link, PingProcessArgs),

    PingRequestTimeout =
        proplists:get_value(ping_request_timeout, ClusterOpts, 1500),

    RecTimer = proplists:get_value(rec_timer, ClusterOpts, []),

    RawReT =
        #rec_timer{
           value     = proplists:get_value(value, RecTimer, 200),
           factor    = proplists:get_value(factor, RecTimer, 2),
           incr      = proplists:get_value(incr, RecTimer, 0),
           max_value = proplists:get_value(max_value, RecTimer, 15000)
          },
    
    State = #state{
               ping_request_timeout = PingRequestTimeout,
               ping_process  = PingProcess,
               rec_timer     = RawReT,
               raw_rec_timer = RawReT,
               cluster_name  = ClusterName,
               pool_name     = PoolName
              },
    OnSuccess =
        fun(St) ->
                riak_pool_balancer:register_pool(ClusterName, PoolName),
                error_logger:info_msg("riak_pool connected ~p ~p",
                                      [ClusterName, PoolName]),
                {ok, connected, St}
        end,
    OnError = fun(St) -> {ok, connecting, St} end,
    check_connecton(State, OnSuccess, OnError).

connecting({timeout, _Ref, ping}, State) ->
    OnSuccess =
        fun(#state{cluster_name = ClusterName, pool_name = PoolName} = St) ->
                riak_pool_balancer:register_pool(ClusterName, PoolName),
                error_logger:info_msg("riak_pool connected ~p ~p",
                                      [ClusterName, PoolName]),
                {next_state, connected, St}
        end,
    OnError = fun(St) -> {next_state, connecting, St} end,
    check_connecton(State, OnSuccess, OnError).

connected({timeout, _Ref, ping}, State)  ->
    OnSuccess = fun(St) -> {next_state, connected, St} end,
    OnError =
        fun(#state{cluster_name = ClusterName, pool_name = PoolName} = St) ->
                riak_pool_balancer:unregister_pool(ClusterName, PoolName),
                {next_state, connecting, St}
        end,
    check_connecton(State, OnSuccess, OnError).

check_connecton(#state{
                   ping_request_timeout = PingRequestTimeout,
                   raw_rec_timer = RawReT,
                   ping_process  = PingProcess,
                   rec_timer     = ReT,
                   cluster_name  = ClusterName,
                   pool_name     = PoolName
                  } = State, OnSuccess, OnError) ->
    case riakc_pb_socket:ping(PingProcess, PingRequestTimeout) of
        pong ->
            gen_fsm:start_timer(RawReT#rec_timer.value, ping),
            OnSuccess(State#state{rec_timer = RawReT});
        Error ->
            error_logger:info_msg("riak_pool connection error  ~p ~p ~p",
                                  [ClusterName, PoolName, Error]),
            {NewValue, NewRet} = timer_value(ReT),
            gen_fsm:start_timer(NewValue, ping),
            OnError(State#state{rec_timer = NewRet})
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{
                                  cluster_name = ClusterName,
                                  pool_name    = PoolName
                                 }) ->
    riak_pool_balancer:unregister_pool(ClusterName, PoolName),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
    
timer_value(#rec_timer{value = Value, max_value = MaxValue} = ReT)
  when Value >= MaxValue ->
    {MaxValue, ReT#rec_timer{value = MaxValue}};
timer_value(#rec_timer{value = Value, factor = Factor, incr = Incr} = ReT) ->
    NewValue = (Value * Factor) + Incr,
    {NewValue, ReT#rec_timer{value = NewValue}}.
