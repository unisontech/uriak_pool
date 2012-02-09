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
         , get_worker/0, free_worker/1
       ]).

call_worker({_Pool, Worker}, Function, Args)->
    apply(riakc_pb_socket , Function, [Worker|Args]).

get_worker()->
    Pool = riak_pool_balancer:get_pool(),
    {Pool, poolboy:checkout(Pool)}.

free_worker({Pool, Worker})->
    poolboy:checkin(Pool, Worker).

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

ping(Worker) ->
  call_worker(Worker, ping, []).
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


get_server_info(Worker) ->
  call_worker(Worker, get_server_info, []).
get_server_info(Worker, A) ->
  call_worker(Worker, get_server_info, [A]).


get(Worker, A, B) ->
  call_worker(Worker, get, [A, B]).
get(Worker, A, B, C) ->
  call_worker(Worker, get, [A, B, C]).
get(Worker, A, B, C, D) ->
  call_worker(Worker, get, [A, B, C, D]).


put(Worker, A) ->
  call_worker(Worker, put, [A]).
put(Worker, A, B) ->
  call_worker(Worker, put, [A, B]).
put(Worker, A, B, C) ->
  call_worker(Worker, put, [A, B, C]).


delete(Worker, A, B) ->
  call_worker(Worker, delete, [A, B]).
delete(Worker, A, B, C) ->
  call_worker(Worker, delete, [A, B, C]).
delete(Worker, A, B, C, D) ->
  call_worker(Worker, delete, [A, B, C, D]).


delete_vclock(Worker, A, B, C) ->
  call_worker(Worker, delete_vclock, [A, B, C]).
delete_vclock(Worker, A, B, C, D) ->
  call_worker(Worker, delete_vclock, [A, B, C, D]).
delete_vclock(Worker, A, B, C, D, E) ->
  call_worker(Worker, delete_vclock, [A, B, C, D, E]).


delete_obj(Worker, A) ->
  call_worker(Worker, delete_obj, [A]).
delete_obj(Worker, A, B) ->
  call_worker(Worker, delete_obj, [A, B]).
delete_obj(Worker, A, B, C) ->
  call_worker(Worker, delete_obj, [A, B, C]).


list_buckets(Worker) ->
  call_worker(Worker, list_buckets, []).
list_buckets(Worker, A) ->
  call_worker(Worker, list_buckets, [A]).
list_buckets(Worker, A, B) ->
  call_worker(Worker, list_buckets, [A, B]).


list_keys(Worker, A) ->
  call_worker(Worker, list_keys, [A]).
list_keys(Worker, A, B) ->
  call_worker(Worker, list_keys, [A, B]).


% Worker doesn't support receiving streaming ops
%stream_list_keys(Worker, A) ->
%  call_worker(Worker, stream_list_keys, [A]).
%stream_list_keys(Worker, A, B) ->
%  call_worker(Worker, stream_list_keys, [A, B]).
%stream_list_keys(Worker, A, B, C) ->
%  call_worker(Worker, stream_list_keys, [A, B, C]).


get_bucket(Worker, A) ->
  call_worker(Worker, get_bucket, [A]).
get_bucket(Worker, A, B) ->
  call_worker(Worker, get_bucket, [A, B]).
get_bucket(Worker, A, B, C) ->
  call_worker(Worker, get_bucket, [A, B, C]).


set_bucket(Worker, A, B) ->
  call_worker(Worker, set_bucket, [A, B]).
set_bucket(Worker, A, B, C) ->
  call_worker(Worker, set_bucket, [A, B, C]).
set_bucket(Worker, A, B, C, D) ->
  call_worker(Worker, set_bucket, [A, B, C, D]).


mapred(Worker, A, B) ->
  call_worker(Worker, mapred, [A, B]).
mapred(Worker, A, B, C) ->
  call_worker(Worker, mapred, [A, B, C]).
mapred(Worker, A, B, C, D) ->
  call_worker(Worker, mapred, [A, B, C, D]).


mapred_stream(Worker, A, B, C) ->
  call_worker(Worker, mapred_stream, [A, B, C]).
mapred_stream(Worker, A, B, C, D) ->
  call_worker(Worker, mapred_stream, [A, B, C, D]).
mapred_stream(Worker, A, B, C, D, E) ->
  call_worker(Worker, mapred_stream, [A, B, C, D, E]).


mapred_bucket(Worker, A, B) ->
  call_worker(Worker, mapred_bucket, [A, B]).
mapred_bucket(Worker, A, B, C) ->
  call_worker(Worker, mapred_bucket, [A, B, C]).
mapred_bucket(Worker, A, B, C, D) ->
  call_worker(Worker, mapred_bucket, [A, B, C, D]).


mapred_bucket_stream(Worker, A, B, C, D) ->
  call_worker(Worker, mapred_bucket_stream, [A, B, C, D]).
mapred_bucket_stream(Worker, A, B, C, D, E) ->
  call_worker(Worker, mapred_bucket_stream, [A, B, C, D, E]).


search(Worker, A, B) ->
  call_worker(Worker, search, [A, B]).
search(Worker, A, B, C, D) ->
  call_worker(Worker, search, [A, B, C, D]).
search(Worker, A, B, C, D, E) ->
  call_worker(Worker, search, [A, B, C, D, E]).


get_index(Worker, A, B, C) ->
  call_worker(Worker, get_index, [A, B, C]).
get_index(Worker, A, B, C, D) ->
  call_worker(Worker, get_index, [A, B, C, D]).

get_index(Worker, A, B, C, D, E) ->
  call_worker(Worker, get_index, [A, B, C, D, E]).
get_index(Worker, A, B, C, D, E, F) ->
  call_worker(Worker, get_index, [A, B, C, D, E, F]).

% another per-connection option
%default_timeout(Worker) ->
%  call_worker(Worker, default_timeout, []).
