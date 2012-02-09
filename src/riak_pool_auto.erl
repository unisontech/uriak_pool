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

ping() ->
  call_worker(ping, []).
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


get_server_info() ->
  call_worker(get_server_info, []).
get_server_info(A) ->
  call_worker(get_server_info, [A]).


get(A, B) ->
  call_worker(get, [A, B]).
get(A, B, C) ->
  call_worker(get, [A, B, C]).
get(A, B, C, D) ->
  call_worker(get, [A, B, C, D]).


put(A) ->
  call_worker(put, [A]).
put(A, B) ->
  call_worker(put, [A, B]).
put(A, B, C) ->
  call_worker(put, [A, B, C]).


delete(A, B) ->
  call_worker(delete, [A, B]).
delete(A, B, C) ->
  call_worker(delete, [A, B, C]).
delete(A, B, C, D) ->
  call_worker(delete, [A, B, C, D]).


delete_vclock(A, B, C) ->
  call_worker(delete_vclock, [A, B, C]).
delete_vclock(A, B, C, D) ->
  call_worker(delete_vclock, [A, B, C, D]).
delete_vclock(A, B, C, D, E) ->
  call_worker(delete_vclock, [A, B, C, D, E]).


delete_obj(A) ->
  call_worker(delete_obj, [A]).
delete_obj(A, B) ->
  call_worker(delete_obj, [A, B]).
delete_obj(A, B, C) ->
  call_worker(delete_obj, [A, B, C]).


list_buckets() ->
  call_worker(list_buckets, []).
list_buckets(A) ->
  call_worker(list_buckets, [A]).
list_buckets(A, B) ->
  call_worker(list_buckets, [A, B]).


list_keys(A) ->
  call_worker(list_keys, [A]).
list_keys(A, B) ->
  call_worker(list_keys, [A, B]).


% Worker doesn't support receiving streaming ops
%stream_list_keys(A) ->
%  call_worker(stream_list_keys, [A]).
%stream_list_keys(A, B) ->
%  call_worker(stream_list_keys, [A, B]).
%stream_list_keys(A, B, C) ->
%  call_worker(stream_list_keys, [A, B, C]).


get_bucket(A) ->
  call_worker(get_bucket, [A]).
get_bucket(A, B) ->
  call_worker(get_bucket, [A, B]).
get_bucket(A, B, C) ->
  call_worker(get_bucket, [A, B, C]).


set_bucket(A, B) ->
  call_worker(set_bucket, [A, B]).
set_bucket(A, B, C) ->
  call_worker(set_bucket, [A, B, C]).
set_bucket(A, B, C, D) ->
  call_worker(set_bucket, [A, B, C, D]).


mapred(A, B) ->
  call_worker(mapred, [A, B]).
mapred(A, B, C) ->
  call_worker(mapred, [A, B, C]).
mapred(A, B, C, D) ->
  call_worker(mapred, [A, B, C, D]).


mapred_stream(A, B, C) ->
  call_worker(mapred_stream, [A, B, C]).
mapred_stream(A, B, C, D) ->
  call_worker(mapred_stream, [A, B, C, D]).
mapred_stream(A, B, C, D, E) ->
  call_worker(mapred_stream, [A, B, C, D, E]).


mapred_bucket(A, B) ->
  call_worker(mapred_bucket, [A, B]).
mapred_bucket(A, B, C) ->
  call_worker(mapred_bucket, [A, B, C]).
mapred_bucket(A, B, C, D) ->
  call_worker(mapred_bucket, [A, B, C, D]).


mapred_bucket_stream(A, B, C, D) ->
  call_worker(mapred_bucket_stream, [A, B, C, D]).
mapred_bucket_stream(A, B, C, D, E) ->
  call_worker(mapred_bucket_stream, [A, B, C, D, E]).


search(A, B) ->
  call_worker(search, [A, B]).
search(A, B, C, D) ->
  call_worker(search, [A, B, C, D]).
search(A, B, C, D, E) ->
  call_worker(search, [A, B, C, D, E]).


get_index(A, B, C) ->
  call_worker(get_index, [A, B, C]).
get_index(A, B, C, D) ->
  call_worker(get_index, [A, B, C, D]).

get_index(A, B, C, D, E) ->
  call_worker(get_index, [A, B, C, D, E]).
get_index(A, B, C, D, E, F) ->
  call_worker(get_index, [A, B, C, D, E, F]).

% another per-connection option
%default_timeout() ->
%  call_worker(default_timeout, []).
