-module(rp_obj).

-extends(riakc_obj).
-export([new/3,
         update_value/2,
         get_value/1,
         get_values/1]).

-include_lib("riakc/include/riakc_obj.hrl").
-define(CTYPE_BINARY, "application/octet-stream").

new(B, K, V) when is_binary(V) -> riakc_obj:new(B,K, V, ?CTYPE_BINARY);
new(B, K, V) -> riakc_obj:new(B, K, V).

update_value(O, V) when is_binary(V) -> riakc_obj:update_value(O, V, ?CTYPE_BINARY);
update_value(O, V) -> riakc_obj:update_value(O, V).    

get_value(O)->
    unpack_binary(O:get_content_type(), O:get_value()).
         
get_values(O)->
    [unpack_binary(CT,V) || {CT, V} <- lists:zip(O:get_content_types(), O:get_values())].
                           
%% --------------------inner funs--------------------
unpack_binary(?CTYPE_ERLANG_BINARY, V) -> binary_to_term(V);
unpack_binary(?CTYPE_BINARY, V) -> V;
unpack_binary(undefined, V) -> binary_to_term(V); %thx to dblayer
unpack_binary(CT, _V) -> throw({unknown_content_type, CT}).
