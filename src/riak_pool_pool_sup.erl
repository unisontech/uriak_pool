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

-module(riak_pool_pool_sup).

-behaviour(supervisor).

-export([
         start_link/3,
         name/2
        ]).

-export([init/1]).


name(ClusterName, {PoolName, _SizeOpts, _PoolOpts}) ->
    name(ClusterName, PoolName);
name(_ClusterName, PoolName) ->
    riak_pool_misc:gen_name(?MODULE, PoolName).

start_link(ClusterName, ClusterOpts, Pool) ->
    Name = name(ClusterName, Pool),
    supervisor:start_link({local, Name}, ?MODULE,
                          [ClusterName, ClusterOpts, Pool]).

init([ClusterName, ClusterOpts, {PoolName, SizeArgs, WorkerArgs}]) ->
    PoolWorkerArgs =
        lists:usort([{auto_reconnect,true},{queue_if_disconnected,true}]),
    NewWorkerArgs =
        lists:merge(PoolWorkerArgs, lists:usort(WorkerArgs)),

    PoolArgs =
        [
         {name, {local, PoolName}}, {worker_module, riak_pool_worker} | SizeArgs
        ],
    PoolWorkersSpecs = poolboy:child_spec(PoolName, PoolArgs, NewWorkerArgs),

    Host = proplists:get_value(host, WorkerArgs),
    Port = proplists:get_value(port, WorkerArgs),

    PingProcessArgs =
        [Host, Port, [{queue_if_disconnected, false}, {auto_reconnect, true}]],

    KeepAlive =
        {riak_pool_keep_alive, {riak_pool_keep_alive, start_link,
                                [ClusterName, ClusterOpts, PoolName,
                                 PingProcessArgs]},
         permanent, 2000, worker, [riak_pool_keep_alive]},

    {ok, {{one_for_all, 10, 10}, [PoolWorkersSpecs, KeepAlive]}}.
