-module(riak_pool).

-export([% set_options/2, set_options/3,
         is_connected/1, is_connected/2,
         ping/1, ping/2,
         % get_client_id/1, get_client_id/2,
         % set_client_id/2, set_client_id/3,
         get_server_info/1, get_server_info/2,
         get/3, get/4, get/5,
         put/2, put/3, put/4,
         delete/3, delete/4, delete/5,
         delete_vclock/4, delete_vclock/5, delete_vclock/6,
         delete_obj/2, delete_obj/3, delete_obj/4,
         list_buckets/1, list_buckets/2, list_buckets/3,
         list_keys/2, list_keys/3,
%         stream_list_keys/2, stream_list_keys/3, stream_list_keys/4,
         get_bucket/2, get_bucket/3, get_bucket/4,
         set_bucket/3, set_bucket/4, set_bucket/5,
         mapred/3, mapred/4, mapred/5,
         mapred_stream/4, mapred_stream/5, mapred_stream/6,
         mapred_bucket/3, mapred_bucket/4, mapred_bucket/5,
         mapred_bucket_stream/5, mapred_bucket_stream/6,
         search/3, search/5, search/6,
         get_index/4, get_index/5, get_index/6, get_index/7
         % default_timeout/1
         , get_worker/0, get_worker/1, free_worker/1
         ,restart/0
       ]).

-type worker() :: {atom(), pid()}.

-spec call_worker(worker(), fun(), [term()]) -> term().
call_worker({_Pool, Worker}, Function, Args)->
    apply(riakc_pb_socket , Function, [Worker|Args]).

-spec get_worker() -> worker().                        
get_worker()->
    Pool = riak_pool_balancer:get_pool(),
    {Pool, poolboy:checkout(Pool)}.

-spec get_worker(AppName :: atom()) -> worker().
get_worker(AppName)->
    Pool = riak_pool_balancer:get_pool(AppName),
    {Pool, poolboy:checkout(Pool)}.

-spec free_worker(worker())-> ok.                         
free_worker({Pool, Worker})->
    poolboy:checkin(Pool, Worker).

restart()->
    riak_pool_sup:restart().

%%%----------------------------------------------------------------------
%%% Riak Calls (can you say auto-generated-code?)
%%%----------------------------------------------------------------------
% set_options is a per-connection setting.  doesn't make sense in a pool.
%set_options(Worker, A) ->
%  call_worker(Worker, set_options, [A]).
%set_options(Worker, A, B) ->
%  call_worker(Worker, set_options, [A, B]).

is_connected(Worker) ->
  call_worker(Worker, is_connected, []).
is_connected(Worker, A) ->
  call_worker(Worker, is_connected, [A]).

-spec ping(worker()) -> ok | {error, term()}.
ping(Worker) ->
  call_worker(Worker, ping, []).
-spec ping(worker(), timeout()) -> ok | {error, term()}.
ping(Worker, A) ->
  call_worker(Worker, ping, [A]).

% get/set client id doesn't make sense for random pool workers
%get_client_id(Worker) ->
%  call_worker(Worker, get_client_id, []).
%get_client_id(Worker, A) ->
%  call_worker(Worker, get_client_id, [A]).


%set_client_id(Worker, A) ->
%  call_worker(Worker, set_client_id, [A]).
%set_client_id(Worker, A, B) ->
%  call_worker(Worker, set_client_id, [A, B]).

-spec get_server_info(worker()) -> {ok, riakc_pb_socket:server_info()} | {error, term()}.
get_server_info(Worker) ->
  call_worker(Worker, get_server_info, []).
-spec get_server_info(worker(), timeout()) -> {ok, riakc_pb_socket:server_info()} | {error, term()}.
get_server_info(Worker, A) ->
  call_worker(Worker, get_server_info, [A]).


-spec get(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string()) -> {ok, riakc_obj:riakc_obj()} | {error, term()}.
get(Worker, A, B) ->
  call_worker(Worker, get, [A, B]).
-spec get(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), timeout() |  riakc_pb_socket:riak_pbc_options()) ->
                 {ok, riakc_obj:riakc_obj()} | {error, term() | unchanged}.
get(Worker, A, B, C) ->
  call_worker(Worker, get, [A, B, C]).
-spec get(worker(), 
          riakc_pb_socket:bucket() | string(), 
          riakc_pb_socket:key() | string(),
          riakc_pb_socket:riak_pbc_options(), 
          timeout()) ->
                 {ok, riakc_obj:riakc_obj()} | 
                 {error, term() | unchanged}.
get(Worker, A, B, C, D) ->
  call_worker(Worker, get, [A, B, C, D]).


-spec put(worker(), riakc_obj:riakc_obj()) -> ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(Worker, A) ->
  call_worker(Worker, put, [A]).
-spec put(worker(), riakc_obj:riakc_obj(), timeout() | riakc_pb_socket:riak_pbc_options()) ->
                 ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(Worker, A, B) ->
  call_worker(Worker, put, [A, B]).
-spec put(worker(), riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options(), timeout()) ->
                 ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(Worker, A, B, C) ->
  call_worker(Worker, put, [A, B, C]).


-spec delete(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string()) -> ok | {error, term()}.
delete(Worker, A, B) ->
  call_worker(Worker, delete, [A, B]).
-spec delete(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(),
             timeout() | riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete(Worker, A, B, C) ->
  call_worker(Worker, delete, [A, B, C]).
-spec delete(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(),
             riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete(Worker, A, B, C, D) ->
  call_worker(Worker, delete, [A, B, C, D]).


-spec delete_vclock(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock()) -> ok | {error, term()}.
delete_vclock(Worker, A, B, C) ->
  call_worker(Worker, delete_vclock, [A, B, C]).
-spec delete_vclock(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock(),
             timeout() | riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete_vclock(Worker, A, B, C, D) ->
  call_worker(Worker, delete_vclock, [A, B, C, D]).
-spec delete_vclock(worker(), riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock(),
             riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete_vclock(Worker, A, B, C, D, E) ->
  call_worker(Worker, delete_vclock, [A, B, C, D, E]).


-spec delete_obj(worker(), riakc_obj:riakc_obj()) -> ok | {error, term()}.
delete_obj(Worker, A) ->
  call_worker(Worker, delete_obj, [A]).
-spec delete_obj(worker(), riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete_obj(Worker, A, B) ->
  call_worker(Worker, delete_obj, [A, B]).
-spec delete_obj(worker(), riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete_obj(Worker, A, B, C) ->
  call_worker(Worker, delete_obj, [A, B, C]).


-spec list_buckets(worker()) -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets(Worker) ->
  call_worker(Worker, list_buckets, []).
-spec list_buckets(worker(), timeout()) -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets(Worker, A) ->
  call_worker(Worker, list_buckets, [A]).
-spec list_buckets(worker(), timeout(), timeout()) -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets(Worker, A, B) ->
  call_worker(Worker, list_buckets, [A, B]).


-spec list_keys(worker(), riakc_pb_socket:bucket()) -> {ok, [riakc_pb_socket:key()]}.
list_keys(Worker, A) ->
  call_worker(Worker, list_keys, [A]).
-spec list_keys(worker(), riakc_pb_socket:bucket(), timeout()) -> {ok, [riakc_pb_socket:key()]}.
list_keys(Worker, A, B) ->
  call_worker(Worker, list_keys, [A, B]).


% Worker doesn't support receiving streaming ops
%stream_list_keys(Worker, A) ->
%  call_worker(Worker, stream_list_keys, [A]).
%stream_list_keys(Worker, A, B) ->
%  call_worker(Worker, stream_list_keys, [A, B]).
%stream_list_keys(Worker, A, B, C) ->
%  call_worker(Worker, stream_list_keys, [A, B, C]).

-spec get_bucket(worker(), riakc_pb_socket:bucket()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(Worker, A) ->
  call_worker(Worker, get_bucket, [A]).
-spec get_bucket(worker(), riakc_pb_socket:bucket(), timeout()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(Worker, A, B) ->
  call_worker(Worker, get_bucket, [A, B]).
-spec get_bucket(worker(), riakc_pb_socket:bucket(), timeout(), timeout()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(Worker, A, B, C) ->
  call_worker(Worker, get_bucket, [A, B, C]).


-spec set_bucket(worker(), riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props()) -> ok | {error, term()}.
set_bucket(Worker, A, B) ->
  call_worker(Worker, set_bucket, [A, B]).
-spec set_bucket(worker(), riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(Worker, A, B, C) ->
  call_worker(Worker, set_bucket, [A, B, C]).
-spec set_bucket(worker(), riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(Worker, A, B, C, D) ->
  call_worker(Worker, set_bucket, [A, B, C, D]).


-spec mapred(Pid :: worker(),
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(Worker, A, B) ->
    call_worker(Worker, mapred, [A, B]).
-spec mapred(Pid :: worker(),
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()],
             TimeoutMillisecs :: integer()  | 'infinity') ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(Worker, A, B, C) ->
    call_worker(Worker, mapred, [A, B, C]).
-spec mapred(Pid :: worker(),
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()],
             TimeoutMillisecs :: integer()  | 'infinity',
             CallTimeoutMillisecs :: integer()  | 'infinity') ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(Worker, A, B, C, D) ->
    call_worker(Worker, mapred, [A, B, C, D]).


-spec mapred_stream(Pid :: worker(),
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: worker()) ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_stream(Worker, A, B, C) ->
    call_worker(Worker, mapred_stream, [A, B, C]).
-spec mapred_stream(Pid :: worker(),
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: worker(),
                    TimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_stream(Worker, A, B, C, D) ->
    call_worker(Worker, mapred_stream, [A, B, C, D]).
-spec mapred_stream(Pid :: worker(),
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: worker(),
                    TimeoutMillisecs :: integer() | 'infinity',
                    CallTimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_stream(Worker, A, B, C, D, E) ->
    call_worker(Worker, mapred_stream, [A, B, C, D, E]).


-spec mapred_bucket(Pid :: worker(),
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_bucket(Worker, A, B) ->
    call_worker(Worker, mapred_bucket, [A, B]).
-spec mapred_bucket(Pid :: worker(),
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    TimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_bucket(Worker, A, B, C) ->
    call_worker(Worker, mapred_bucket, [A, B, C]).
-spec mapred_bucket(Pid :: worker(),
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    TimeoutMillisecs :: integer() | 'infinity',
                    CallTimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                           {error, Err :: term()}.
mapred_bucket(Worker, A, B, C, D) ->
    call_worker(Worker, mapred_bucket, [A, B, C, D]).


-spec mapred_bucket_stream(Pid :: worker(),
                           Bucket :: riakc_pb_socket:bucket(),
                           Query :: [riak_kv_mapred_query:mapred_queryterm()],
                           ClientPid :: worker(),
                           TimeoutMillisecs :: integer() | 'infinity') ->
                                  {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                                  {error, Err :: term()}.
mapred_bucket_stream(Worker, A, B, C, D) ->
    call_worker(Worker, mapred_bucket_stream, [A, B, C, D]).
-spec mapred_bucket_stream(Pid :: worker(),
                           Bucket :: riakc_pb_socket:bucket(),
                           Query :: [riak_kv_mapred_query:mapred_queryterm()],
                           ClientPid :: worker(),
                           TimeoutMillisecs :: integer() | 'infinity',
                           CallTimeoutMillisecs :: integer() | 'infinity') ->
                                  {ok, {ReqId :: term(), MR_FSM_PID :: worker()}} |
                                  {error, Err :: term()}.
mapred_bucket_stream(Worker, A, B, C, D, E) ->
    call_worker(Worker, mapred_bucket_stream, [A, B, C, D, E]).


-spec search(worker(), riakc_pb_socket:bucket(), string()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(Worker, A, B) ->
    call_worker(Worker, search, [A, B]).
-spec search(worker(), riakc_pb_socket:bucket(), string(),
             [rhc_mapred:query_part()], integer()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(Worker, A, B, C, D) ->
    call_worker(Worker, search, [A, B, C, D]).
-spec search(worker(), riakc_pb_socket:bucket(), string(),
             [rhc_mapred:query_part()], integer(), integer()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(Worker, A, B, C, D, E) ->
    call_worker(Worker, search, [A, B, C, D, E]).


-spec get_index(worker(), riakc_pb_socket:bucket(), Index::binary(), Key::binary()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(Worker, A, B, C) ->
    call_worker(Worker, get_index, [A, B, C]).
-spec get_index(worker(), riakc_pb_socket:bucket(), Index::binary(), Key::binary(), integer(), integer()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(Worker, A, B, C, D) ->
    call_worker(Worker, get_index, [A, B, C, D]).
-spec get_index(worker(), riakc_pb_socket:bucket(), Index::binary(), StartKey::binary(), EndKey::binary()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(Worker, A, B, C, D, E) ->
    call_worker(Worker, get_index, [A, B, C, D, E]).
-spec get_index(worker(), riakc_pb_socket:bucket(), Index::binary(), StartKey::binary(), EndKey::binary(), integer(), integer()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(Worker, A, B, C, D, E, F) ->
    call_worker(Worker, get_index, [A, B, C, D, E, F]).

% another per-connection option
%default_timeout(Worker) ->
%  call_worker(Worker, default_timeout, []).
