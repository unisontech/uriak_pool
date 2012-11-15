#!/bin/sh

exec erl -pa ebin deps/*/ebin -boot start_sasl \
    -sname riak_pool_dev +K true \
    -s riak_pool_app \
    -setcookie blah \
    -config ./etc/app 
