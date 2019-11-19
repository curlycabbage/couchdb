% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_expiring_cache_fdb).

-export([
    insert/5,
    lookup/2,
    clear_expired_range/2
]).


-define(XC, 53). % coordinate with fabric2.hrl
-define(PK, 1).
-define(EXP, 2).


-include_lib("couch_expiring_cache/include/couch_expiring_cache.hrl").


% Data model
% see: https://forums.foundationdb.org/t/designing-key-value-expiration-in-fdb/156
%
% (?XC, ?PK, Name, Key) := (Val, StaleTS, ExpireTS)
% (?XC, ?EXP, ExpireTS, Name, Key) := ()


insert(Name, Key, Val, StaleTS, ExpiresTS) ->
    LayerPrefix = couch_expiring_cache_server:layer_prefix(),
    PK = primary_key(Name, Key, LayerPrefix),
    PV = erlfdb_tuple:pack({Val, StaleTS, ExpiresTS}),
    XK = expiry_key(ExpiresTS, Name, Key, LayerPrefix),
    XV = erlfdb_tuple:pack({}),
    fabric2_fdb:transactional(fun(Tx) ->
        ok = erlfdb:set(Tx, PK, PV),
        ok = erlfdb:set(Tx, XK, XV)
    end).


lookup(Name, Key) ->
    LayerPrefix = couch_expiring_cache_server:layer_prefix(),
    PK = primary_key(Name, Key, LayerPrefix),
    fabric2_fdb:transactional(fun(Tx) ->
        case erlfdb:wait(erlfdb:get(Tx, PK)) of
            not_found ->
                not_found;
            Bin when is_binary(Bin) ->
                {Val, StaleTS, ExpiresTS} = erlfdb_tuple:unpack(Bin),
                Now = erlang:system_time(?TIME_UNIT),
                if
                    Now < StaleTS -> {fresh, Val};
                    Now < ExpiresTS -> {stale, Val};
                    true -> expired
                end
        end
    end).


clear_expired_range(EndTS, Limit) when Limit > 0 ->
    Callback = fun
        (TS, 0) when TS > 0 -> TS;
        (TS, Acc) -> min(TS, Acc)
    end,
    LayerPrefix = couch_expiring_cache_server:layer_prefix(),
    ExpiresPrefix = erlfdb_tuple:pack({?XC, ?EXP}, LayerPrefix),
    fabric2_fdb:transactional(fun(Tx) ->
        fabric2_fdb:fold_range({tx, Tx}, ExpiresPrefix, fun({K, _V}, Acc) ->
            Unpacked = erlfdb_tuple:unpack(K, ExpiresPrefix),
            couch_log:info("~p clearing ~p", [?MODULE, Unpacked]),
            {ExpiresTS, Name, Key} = Unpacked,
            clear_expired(Tx, ExpiresTS, Name, Key, LayerPrefix),
            Callback(ExpiresTS, Acc)
        end, 0, [{end_key, EndTS}, {limit, Limit}])
    end).


%% Private


clear_expired(Tx, ExpiresTS, Name, Key, Prefix) ->
    PK = primary_key(Name, Key, Prefix),
    XK = expiry_key(ExpiresTS, Name, Key, Prefix),
    ok = erlfdb:clear(Tx, PK),
    ok = erlfdb:clear(Tx, XK).


primary_key(Name, Key, Prefix) ->
    erlfdb_tuple:pack({?XC, ?PK, Name, Key}, Prefix).


expiry_key(ExpiresTS, Name, Key, Prefix) ->
    erlfdb_tuple:pack({?XC, ?EXP, ExpiresTS, Name, Key}, Prefix).
