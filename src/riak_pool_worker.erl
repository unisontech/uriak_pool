-module(riak_pool_worker).
-export([start_link/1]).

start_link(Args) -> 
    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    Options = [{queue_if_disconnected, proplists:get_value(queue_if_disconnected, Args)},
               {auto_reconnect, proplists:get_value(auto_reconnect, Args)}],
    riakc_pb_socket:start_link(Host, Port, Options).

