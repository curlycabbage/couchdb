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

-module(couch_expiring_cache_server).

-behaviour(gen_server).

-export([
    start_link/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    layer_prefix/0
]).


-define(DEFAULT_BATCH_SIZE, 1000).
-define(DEFAULT_PERIOD_MS, 5000).
-define(DEFAULT_MAX_JITTER_MS, 1000).


-include_lib("couch_expiring_cache/include/couch_expiring_cache.hrl").


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    true = set_layer_prefix(),
    {ok, #{
        timer_ref => schedule_remove_expired(),
        last_removal => 0,
        oldest_ts => 0,
        lag => 0}}.


terminate(_, _) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(remove_expired, St = #{oldest_ts := OldestTS0}) ->
    Now = erlang:system_time(?TIME_UNIT),
    OldestTS = max(OldestTS0, remove_expired(Now)),
    Ref = schedule_remove_expired(),
    {noreply, St#{
        timer_ref := Ref,
        last_removal := Now,
        oldest_ts := OldestTS,
        lag := Now - OldestTS}};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


layer_prefix() ->
    [{layer_prefix, Prefix}] = ets:lookup(?MODULE, layer_prefix),
    Prefix.


%% Private


set_layer_prefix() ->
    fabric2_fdb:transactional(fun(Tx) ->
        LayerPrefix = fabric2_fdb:get_dir(Tx),
        true = ets:insert(?MODULE, {layer_prefix, LayerPrefix})
    end).


remove_expired(EndTS) ->
    couch_expiring_cache_fdb:clear_expired_range(EndTS, batch_size()).


schedule_remove_expired() ->
    Timeout = period_ms(),
    MaxJitter = max(Timeout div 2, max_jitter_ms()),
    Wait = Timeout + rand:uniform(max(1, MaxJitter)),
    erlang:send_after(Wait, self(), remove_expired).


period_ms() ->
    config:get_integer("couch_expiring_cache", "period_ms",
        ?DEFAULT_PERIOD_MS).


max_jitter_ms() ->
    config:get_integer("couch_expiring_cache", "max_jitter_ms",
        ?DEFAULT_MAX_JITTER_MS).


batch_size() ->
    config:get_integer("couch_expiring_cache", "batch_size",
        ?DEFAULT_BATCH_SIZE).
