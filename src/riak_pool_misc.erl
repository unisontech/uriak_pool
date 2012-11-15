-module(riak_pool_misc).

-export([
         gen_name/2
        ]).

gen_name(Module, ClusterName) ->
    list_to_atom(atom_to_list(Module) ++ "_" ++ atom_to_list(ClusterName)).
