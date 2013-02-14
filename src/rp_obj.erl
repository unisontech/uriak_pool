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

-module(rp_obj).

-extends(riakc_obj).
-export([new/3,
         update_value/2,
         get_value/1,
         get_values/1]).

-include("rp_obj.hrl").

-spec new(binary(), binary(), term()) -> tuple().
new(B, K, V) ->
    {CT, Val} = pack_binary(V),
    riakc_obj:new(B, K, Val, CT).

-spec update_value(tuple(), term()) -> tuple().
update_value(O, V) ->
    {CT, Val} = pack_binary(V),
    riakc_obj:update_value(O, Val, CT).

-spec get_value(tuple())-> term().
get_value(O)->
    unpack_binary(O:get_content_type(), O:get_value()).

-spec get_values(tuple())-> [term()].
get_values(O)->
    [unpack_binary(CT,V) || {CT, V} <- lists:zip(O:get_content_types(), O:get_values())].

%% --------------------inner funs--------------------
unpack_binary(?CTYPE_ERLANG_BINARY, V) -> binary_to_term(V);
unpack_binary(?CTYPE_BINARY, V) -> V;
unpack_binary(undefined, V) -> binary_to_term(V); %thx to dblayer
unpack_binary(CT, _V) -> throw({unknown_content_type, CT}).


%%explicit doubling of logic from riak_pb_kv_codec
-spec pack_binary(term())-> {list(), binary()}.
pack_binary(Value) when is_binary(Value) ->
    {?CTYPE_BINARY, Value};
pack_binary(Value) ->
    {?CTYPE_ERLANG_BINARY, term_to_binary(Value)}.
