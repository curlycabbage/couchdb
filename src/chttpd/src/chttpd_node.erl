% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(chttpd_node).

-export([
    handle_node_req/1
]).

-include_lib("couch/include/couch_db.hrl").

-import(chttpd,
    [send_json/2,send_json/3,send_method_not_allowed/2,
    send_chunk/2,start_chunked_response/3]).

% Node-specific request handler (_config and _stats)
% Support _local meaning this node
handle_node_req(#httpd{path_parts=[_, <<"_local">>]}=Req) ->
    send_json(Req, 200, {[{name, node()}]});
handle_node_req(#httpd{path_parts=[A, <<"_local">>|Rest]}=Req) ->
    handle_node_req(Req#httpd{path_parts=[A, node()] ++ Rest});
% GET /_node/$node/_config
handle_node_req(#httpd{method='GET', path_parts=[_, Node, <<"_config">>]}=Req) ->
    Grouped = lists:foldl(fun({{Section, Key}, Value}, Acc) ->
        case dict:is_key(Section, Acc) of
        true ->
            dict:append(Section, {list_to_binary(Key), list_to_binary(Value)}, Acc);
        false ->
            dict:store(Section, [{list_to_binary(Key), list_to_binary(Value)}], Acc)
        end
    end, dict:new(), call_node(Node, config, all, [])),
    KVs = dict:fold(fun(Section, Values, Acc) ->
        [{list_to_binary(Section), {Values}} | Acc]
    end, [], Grouped),
    send_json(Req, 200, {KVs});
handle_node_req(#httpd{path_parts=[_, _Node, <<"_config">>]}=Req) ->
    send_method_not_allowed(Req, "GET");
% GET /_node/$node/_config/Section
handle_node_req(#httpd{method='GET', path_parts=[_, Node, <<"_config">>, Section]}=Req) ->
    KVs = [{list_to_binary(Key), list_to_binary(Value)}
            || {Key, Value} <- call_node(Node, config, get, [Section])],
    send_json(Req, 200, {KVs});
handle_node_req(#httpd{path_parts=[_, _Node, <<"_config">>, _Section]}=Req) ->
    send_method_not_allowed(Req, "GET");
% PUT /_node/$node/_config/Section/Key
% "value"
handle_node_req(#httpd{method='PUT', path_parts=[_, Node, <<"_config">>, Section, Key]}=Req) ->
    couch_util:check_config_blacklist(Section),
    Value = couch_util:trim(chttpd:json_body(Req)),
    Persist = chttpd:header_value(Req, "X-Couch-Persist") /= "false",
    OldValue = call_node(Node, config, get, [Section, Key, ""]),
    case call_node(Node, config, set, [Section, Key, ?b2l(Value), Persist]) of
        ok ->
            send_json(Req, 200, list_to_binary(OldValue));
        {error, Reason} ->
            chttpd:send_error(Req, {bad_request, Reason})
    end;
% GET /_node/$node/_config/Section/Key
handle_node_req(#httpd{method='GET', path_parts=[_, Node, <<"_config">>, Section, Key]}=Req) ->
    case call_node(Node, config, get, [Section, Key, undefined]) of
    undefined ->
        throw({not_found, unknown_config_value});
    Value ->
        send_json(Req, 200, list_to_binary(Value))
    end;
% DELETE /_node/$node/_config/Section/Key
handle_node_req(#httpd{method='DELETE',path_parts=[_, Node, <<"_config">>, Section, Key]}=Req) ->
    couch_util:check_config_blacklist(Section),
    Persist = chttpd:header_value(Req, "X-Couch-Persist") /= "false",
    case call_node(Node, config, get, [Section, Key, undefined]) of
    undefined ->
        throw({not_found, unknown_config_value});
    OldValue ->
        case call_node(Node, config, delete, [Section, Key, Persist]) of
            ok ->
                send_json(Req, 200, list_to_binary(OldValue));
            {error, Reason} ->
                chttpd:send_error(Req, {bad_request, Reason})
        end
    end;
handle_node_req(#httpd{path_parts=[_, _Node, <<"_config">>, _Section, _Key]}=Req) ->
    send_method_not_allowed(Req, "GET,PUT,DELETE");
handle_node_req(#httpd{path_parts=[_, _Node, <<"_config">>, _Section, _Key | _]}=Req) ->
    chttpd:send_error(Req, not_found);
% GET /_node/$node/_stats
handle_node_req(#httpd{method='GET', path_parts=[_, Node, <<"_stats">> | Path]}=Req) ->
    flush(Node, Req),
    Stats0 = call_node(Node, couch_stats, fetch, []),
    Stats = couch_stats_httpd:transform_stats(Stats0),
    Nested = couch_stats_httpd:nest(Stats),
    EJSON0 = couch_stats_httpd:to_ejson(Nested),
    EJSON1 = couch_stats_httpd:extract_path(Path, EJSON0),
    chttpd:send_json(Req, EJSON1);
handle_node_req(#httpd{path_parts=[_, _Node, <<"_stats">>]}=Req) ->
    send_method_not_allowed(Req, "GET");
% GET /_node/$node/_system
handle_node_req(#httpd{method='GET', path_parts=[_, Node, <<"_system">>]}=Req) ->
    Stats = call_node(Node, chttpd_misc, get_stats, []),
    EJSON = couch_stats_httpd:to_ejson(Stats),
    send_json(Req, EJSON);
handle_node_req(#httpd{path_parts=[_, _Node, <<"_system">>]}=Req) ->
    send_method_not_allowed(Req, "GET");
% POST /_node/$node/_restart
handle_node_req(#httpd{method='POST', path_parts=[_, Node, <<"_restart">>]}=Req) ->
    call_node(Node, init, restart, []),
    send_json(Req, 200, {[{ok, true}]});
handle_node_req(#httpd{path_parts=[_, _Node, <<"_restart">>]}=Req) ->
    send_method_not_allowed(Req, "POST");
handle_node_req(#httpd{path_parts=[_]}=Req) ->
    chttpd:send_error(Req, {bad_request, <<"Incomplete path to _node request">>});
handle_node_req(#httpd{path_parts=[_, _Node]}=Req) ->
    chttpd:send_error(Req, {bad_request, <<"Incomplete path to _node request">>});
handle_node_req(Req) ->
    chttpd:send_error(Req, not_found).


call_node(Node0, Mod, Fun, Args) when is_binary(Node0) ->
    Node1 = try
                list_to_existing_atom(?b2l(Node0))
            catch
                error:badarg ->
                    throw({not_found, <<"no such node: ", Node0/binary>>})
            end,
    call_node(Node1, Mod, Fun, Args);
call_node(Node, Mod, Fun, Args) when is_atom(Node) ->
    case rpc:call(Node, Mod, Fun, Args) of
        {badrpc, nodedown} ->
            Reason = ?l2b(io_lib:format("~s is down", [Node])),
            throw({error, {nodedown, Reason}});
        Else ->
            Else
    end.

flush(Node, Req) ->
    case couch_util:get_value("flush", chttpd:qs(Req)) of
        "true" ->
            call_node(Node, couch_stats_aggregator, flush, []);
        _Else ->
            ok
    end.
