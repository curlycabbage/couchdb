-module(couch_expiring_cache).

-export([
    lookup/2,
    insert/5
]).


-type millisecond() :: non_neg_integer().


-spec insert(Name :: binary(), Key :: binary(), Value :: binary(),
        StaleTS :: millisecond(), ExpiresTS :: millisecond()) -> ok.
insert(Name, Key, Value, StaleTS, ExpiresTS) ->
    couch_expiring_cache_fdb:insert(Name, Key, Value, StaleTS, ExpiresTS).


-spec lookup(Name :: binary(), Key :: binary()) ->
    not_found | {fresh, Val :: binary()} | {stale, Val :: binary()} | expired.
lookup(Name, Key) ->
    couch_expiring_cache_fdb:lookup(Name, Key).
