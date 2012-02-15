-module(riak_pool_auto).

-export([% set_options/1, set_options/2,
         is_connected/0, is_connected/1,
         ping/0, ping/1,
         % get_client_id/0, get_client_id/1,
         % set_client_id/1, set_client_id/2,
         get_server_info/0, get_server_info/1,
         get/2, get/3, get/4,
         put/1, put/2, put/3,
         delete/2, delete/3, delete/4,
         delete_vclock/3, delete_vclock/4, delete_vclock/5,
         delete_obj/1, delete_obj/2, delete_obj/3,
         list_buckets/0, list_buckets/1, list_buckets/2,
         list_keys/1, list_keys/2,
%         stream_list_keys/1, stream_list_keys/2, stream_list_keys/3,
         get_bucket/1, get_bucket/2, get_bucket/3,
         set_bucket/2, set_bucket/3, set_bucket/4,
         mapred/2, mapred/3, mapred/4,
         mapred_stream/3, mapred_stream/4, mapred_stream/5,
         mapred_bucket/2, mapred_bucket/3, mapred_bucket/4,
         mapred_bucket_stream/4, mapred_bucket_stream/5,
         search/2, search/4, search/5,
         get_index/3, get_index/4, get_index/5, get_index/6
         % default_timeout/0
       ]).

call_worker(Function, Args)->
    Pool = riak_pool_balancer:get_pool(),
    Worker = poolboy:checkout(Pool),
    Reply = apply(riakc_pb_socket , Function, [Worker|Args]),
    poolboy:checkin(Pool, Worker),
    Reply.


%%%----------------------------------------------------------------------
%%% Riak Calls (can you say auto-generated-code?)
%%%----------------------------------------------------------------------
% set_options is a per-connection setting.  doesn't make sense in a pool.
%set_options(A) ->
%  call_worker(set_options, [A]).
%set_options(A, B) ->
%  call_worker(set_options, [A, B]).

is_connected() ->
  call_worker(is_connected, []).
is_connected(A) ->
  call_worker(is_connected, [A]).

-spec ping() -> ok | {error, term()}.
ping() ->
  call_worker(ping, []).
-spec ping(timeout()) -> ok | {error, term()}.
ping(A) ->
  call_worker(ping, [A]).

% get/set client id doesn't make sense for random pool workers
%get_client_id() ->
%  call_worker(get_client_id, []).
%get_client_id(A) ->
%  call_worker(get_client_id, [A]).


%set_client_id(A) ->
%  call_worker(set_client_id, [A]).
%set_client_id(A, B) ->
%  call_worker(set_client_id, [A, B]).

-spec get_server_info() -> {ok, riakc_pb_socket:server_info()} | {error, term()}.
get_server_info() ->
  call_worker(get_server_info, []).
-spec get_server_info(timeout()) -> {ok, riakc_pb_socket:server_info()} | {error, term()}.
get_server_info(A) ->
  call_worker(get_server_info, [A]).


-spec get(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string()) -> {ok, riakc_obj:riakc_obj()} | {error, term()}.
get(A, B) ->
  call_worker(get, [A, B]).
-spec get(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), timeout() |  riakc_pb_socket:riak_pbc_options()) ->
                 {ok, riakc_obj:riakc_obj()} | {error, term() | unchanged}.
get(A, B, C) ->
  call_worker(get, [A, B, C]).
-spec get(
          riakc_pb_socket:bucket() | string(), 
          riakc_pb_socket:key() | string(),
          riakc_pb_socket:riak_pbc_options(), 
          timeout()) ->
                 {ok, riakc_obj:riakc_obj()} | 
                 {error, term() | unchanged}.
get(A, B, C, D) ->
  call_worker(get, [A, B, C, D]).


-spec put(riakc_obj:riakc_obj()) -> ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(A) ->
  call_worker(put, [A]).
-spec put(riakc_obj:riakc_obj(), timeout() | riakc_pb_socket:riak_pbc_options()) ->
                 ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(A, B) ->
  call_worker(put, [A, B]).
-spec put(riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options(), timeout()) ->
                 ok | {ok, riakc_obj:riakc_obj()} | {ok, riakc_pb_socket:key()} | {error, term()}.
put(A, B, C) ->
  call_worker(put, [A, B, C]).


-spec delete(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string()) -> ok | {error, term()}.
delete(A, B) ->
  call_worker(delete, [A, B]).
-spec delete(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(),
             timeout() | riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete(A, B, C) ->
  call_worker(delete, [A, B, C]).
-spec delete(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(),
             riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete(A, B, C, D) ->
  call_worker(delete, [A, B, C, D]).


-spec delete_vclock(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock()) -> ok | {error, term()}.
delete_vclock(A, B, C) ->
  call_worker(delete_vclock, [A, B, C]).
-spec delete_vclock(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock(),
             timeout() | riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete_vclock(A, B, C, D) ->
  call_worker(delete_vclock, [A, B, C, D]).
-spec delete_vclock(riakc_pb_socket:bucket() | string(), riakc_pb_socket:key() | string(), vclock:vclock(),
             riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete_vclock(A, B, C, D, E) ->
  call_worker(delete_vclock, [A, B, C, D, E]).


-spec delete_obj(riakc_obj:riakc_obj()) -> ok | {error, term()}.
delete_obj(A) ->
  call_worker(delete_obj, [A]).
-spec delete_obj(riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options()) -> ok | {error, term()}.
delete_obj(A, B) ->
  call_worker(delete_obj, [A, B]).
-spec delete_obj(riakc_obj:riakc_obj(), riakc_pb_socket:riak_pbc_options(), timeout()) -> ok | {error, term()}.
delete_obj(A, B, C) ->
  call_worker(delete_obj, [A, B, C]).


-spec list_buckets() -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets() ->
  call_worker(list_buckets, []).
-spec list_buckets(timeout()) -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets(A) ->
  call_worker(list_buckets, [A]).
-spec list_buckets(timeout(), timeout()) -> {ok, [riakc_pb_socket:bucket()]} | {error, term()}.
list_buckets(A, B) ->
  call_worker(list_buckets, [A, B]).


-spec list_keys(riakc_pb_socket:bucket()) -> {ok, [riakc_pb_socket:key()]}.
list_keys(A) ->
  call_worker(list_keys, [A]).
-spec list_keys(riakc_pb_socket:bucket(), timeout()) -> {ok, [riakc_pb_socket:key()]}.
list_keys(A, B) ->
  call_worker(list_keys, [A, B]).


% Worker doesn't support receiving streaming ops
%stream_list_keys(A) ->
%  call_worker(stream_list_keys, [A]).
%stream_list_keys(A, B) ->
%  call_worker(stream_list_keys, [A, B]).
%stream_list_keys(A, B, C) ->
%  call_worker(stream_list_keys, [A, B, C]).

-spec get_bucket(riakc_pb_socket:bucket()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(A) ->
  call_worker(get_bucket, [A]).
-spec get_bucket(riakc_pb_socket:bucket(), timeout()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(A, B) ->
  call_worker(get_bucket, [A, B]).
-spec get_bucket(riakc_pb_socket:bucket(), timeout(), timeout()) -> {ok, riakc_pb_socket:bucket_props()} | {error, term()}.
get_bucket(A, B, C) ->
  call_worker(get_bucket, [A, B, C]).


-spec set_bucket(riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props()) -> ok | {error, term()}.
set_bucket(A, B) ->
  call_worker(set_bucket, [A, B]).
-spec set_bucket(riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(A, B, C) ->
  call_worker(set_bucket, [A, B, C]).
-spec set_bucket(riakc_pb_socket:bucket(), riakc_pb_socket:bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(A, B, C, D) ->
  call_worker(set_bucket, [A, B, C, D]).


-spec mapred(
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(A, B) ->
    call_worker(mapred, [A, B]).
-spec mapred(
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()],
             TimeoutMillisecs :: integer()  | 'infinity') ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(A, B, C) ->
    call_worker(mapred, [A, B, C]).
-spec mapred(
             Inputs :: list(),
             Query :: [riak_kv_mapred_query:mapred_queryterm()],
             TimeoutMillisecs :: integer()  | 'infinity',
             CallTimeoutMillisecs :: integer()  | 'infinity') ->
                    {ok, riak_kv_mapred_query:mapred_result()} |
                    {error, timeout} |
                    {error, Err :: term()}.
mapred(A, B, C, D) ->
    call_worker(mapred, [A, B, C, D]).


-spec mapred_stream(
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: pid()) ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_stream(A, B, C) ->
    call_worker(mapred_stream, [A, B, C]).
-spec mapred_stream(
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: pid(),
                    TimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_stream(A, B, C, D) ->
    call_worker(mapred_stream, [A, B, C, D]).
-spec mapred_stream(
                    Inputs :: list(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    ClientPid :: pid(),
                    TimeoutMillisecs :: integer() | 'infinity',
                    CallTimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_stream(A, B, C, D, E) ->
    call_worker(mapred_stream, [A, B, C, D, E]).


-spec mapred_bucket(
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_bucket(A, B) ->
    call_worker(mapred_bucket, [A, B]).
-spec mapred_bucket(
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    TimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_bucket(A, B, C) ->
    call_worker(mapred_bucket, [A, B, C]).
-spec mapred_bucket(
                    Bucket :: riakc_pb_socket:bucket(),
                    Query :: [riak_kv_mapred_query:mapred_queryterm()],
                    TimeoutMillisecs :: integer() | 'infinity',
                    CallTimeoutMillisecs :: integer() | 'infinity') ->
                           {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                           {error, Err :: term()}.
mapred_bucket(A, B, C, D) ->
    call_worker(mapred_bucket, [A, B, C, D]).


-spec mapred_bucket_stream(
                           Bucket :: riakc_pb_socket:bucket(),
                           Query :: [riak_kv_mapred_query:mapred_queryterm()],
                           ClientPid :: pid(),
                           TimeoutMillisecs :: integer() | 'infinity') ->
                                  {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                                  {error, Err :: term()}.
mapred_bucket_stream(A, B, C, D) ->
    call_worker(mapred_bucket_stream, [A, B, C, D]).
-spec mapred_bucket_stream(
                           Bucket :: riakc_pb_socket:bucket(),
                           Query :: [riak_kv_mapred_query:mapred_queryterm()],
                           ClientPid :: pid(),
                           TimeoutMillisecs :: integer() | 'infinity',
                           CallTimeoutMillisecs :: integer() | 'infinity') ->
                                  {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
                                  {error, Err :: term()}.
mapred_bucket_stream(A, B, C, D, E) ->
    call_worker(mapred_bucket_stream, [A, B, C, D, E]).


-spec search(riakc_pb_socket:bucket(), string()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(A, B) ->
    call_worker(search, [A, B]).
-spec search(riakc_pb_socket:bucket(), string(),
             [rhc_mapred:query_part()], integer()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(A, B, C, D) ->
    call_worker(search, [A, B, C, D]).
-spec search(riakc_pb_socket:bucket(), string(),
             [rhc_mapred:query_part()], integer(), integer()) ->
                    {ok, [rhc_mapred:phase_result()]}|{error, term()}.
search(A, B, C, D, E) ->
    call_worker(search, [A, B, C, D, E]).


-spec get_index(riakc_pb_socket:bucket(), Index::binary(), Key::binary()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(A, B, C) ->
    call_worker(get_index, [A, B, C]).
-spec get_index(riakc_pb_socket:bucket(), Index::binary(), Key::binary(), integer(), integer()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(A, B, C, D) ->
    call_worker(get_index, [A, B, C, D]).
-spec get_index(riakc_pb_socket:bucket(), Index::binary(), StartKey::binary(), EndKey::binary()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(A, B, C, D, E) ->
    call_worker(get_index, [A, B, C, D, E]).
-spec get_index(riakc_pb_socket:bucket(), Index::binary(), StartKey::binary(), EndKey::binary(), integer(), integer()) ->
                       {ok, [rhc_mapred:phase_result()]}|{error, term()}.
get_index(A, B, C, D, E, F) ->
    call_worker(get_index, [A, B, C, D, E, F]).

% another per-connection option
%default_timeout() ->
%  call_worker(default_timeout, []).
