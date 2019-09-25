%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Jun 2018 8:47 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_test_topic_owner).

-include_lib("eunit/include/eunit.hrl").

-export([
  setup/0,
  teardown/1,
  ensure_started_test_/0,
  put_values_test_/0,
  put_entries_test_/0,
  seek_min_offset_since_test_/0,
  seek_max_offset_until_test_/0
]).

setup() ->
  {ok, DbRef} = maxwell_store_test_utils:setup(),
  DbRef.

teardown(DbRef) ->
  maxwell_store_test_utils:teardown(DbRef).

ensure_started_test_() ->
  {setup, fun setup/0, fun teardown/1, fun ensure_started/1}.

put_values_test_() ->
  {setup, fun setup/0, fun teardown/1, fun put_values/1}.

put_entries_test_() ->
  {setup, fun setup/0, fun teardown/1, fun put_entries/1}.

seek_min_offset_since_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_min_offset_since/1}.

seek_max_offset_until_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_max_offset_until/1}.

ensure_started(_DbRef) ->
  Topic = <<"topic">>,
  Topic2 = <<"topic_2">>,
  Topic3 = <<"topic_3">>,
  Result = maxwell_store_topic_owner:ensure_started(Topic),
  Result2 = maxwell_store_topic_owner:ensure_started(Topic2),
  Result3 = maxwell_store_topic_owner:ensure_started(Topic3),
  Result4 = maxwell_store_topic_owner:ensure_started(Topic3),
  [
    ?_assertMatch({ok, _}, Result),
    ?_assertMatch({ok, _}, Result2),
    ?_assertMatch({ok, _}, Result3),
    ?_assertMatch({ok, _}, Result4)
  ].

put_values(_DbRef) ->
  Topic = <<"test">>,
  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  Result = maxwell_store_topic_owner:put_values(Topic, [<<"Value_0">>]),
  Result2 = maxwell_store_topic_owner:put_values(Topic, [<<"Value_1">>]),
  Result3 = maxwell_store_topic_owner:put_values(Topic, [<<"Value_2">>, <<"Value_3">>]),
  Result4 = maxwell_store_topic_owner:get_from(Topic, 0, 3),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch(ok, Result3),
    ?_assertMatch([{0, <<"Value_0">>, _}, {1, <<"Value_1">>, _}, {2, <<"Value_2">>, _}], Result4)
  ].

put_entries(_DbRef) ->
  Topic = <<"test">>,
  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  Result = maxwell_store_topic_owner:put_entries(Topic, [{1024, <<"Value_1024">>}]),
  Result2 = maxwell_store_topic_owner:put_entries(Topic, [{1026, <<"Value_1026">>}, {1028, <<"Value_1028">>}]),
  Result3 = maxwell_store_topic_owner:put_entries(Topic, [{1034, <<"Value_1034">>}, {1032, <<"Value_1032">>}]),
  Result4 = maxwell_store_topic_owner:get_from(Topic, 0, 10),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch(ok, Result3),
    ?_assertMatch([
      {1024, <<"Value_1024">>, _}, {1026, <<"Value_1026">>, _},
      {1028, <<"Value_1028">>, _}, {1032, <<"Value_1032">>, _},
      {1034, <<"Value_1034">>, _}
    ], Result4)
  ].

seek_min_offset_since(_DbRef) ->
  Topic = <<"test">>,

  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  Timestamp0 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_1">>]),
  timer:sleep(200),
  Timestamp1 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_2">>]),
  timer:sleep(200),
  Timestamp2 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_3">>]),
  timer:sleep(200),
  Timestamp3 = maxwell_store_test_utils:get_current_timestamp(),

  Result = maxwell_store_topic_owner:seek_min_offset_since(Topic, Timestamp0),
  Result2 = maxwell_store_topic_owner:seek_min_offset_since(Topic, Timestamp1),
  Result3 = maxwell_store_topic_owner:seek_min_offset_since(Topic, Timestamp2),
  Result4 = maxwell_store_topic_owner:seek_min_offset_since(Topic, Timestamp3),
  [
    ?_assertMatch({ok, 0}, Result),
    ?_assertMatch({ok, 1}, Result2),
    ?_assertMatch({ok, 2}, Result3),
    ?_assertMatch(undefined, Result4)
  ].

seek_max_offset_until(_DbRef) ->
  Topic = <<"test">>,

  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  Timestamp0 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_1">>]),
  timer:sleep(200),
  Timestamp1 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_2">>]),
  timer:sleep(200),
  Timestamp2 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(200),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<"Value_3">>]),
  timer:sleep(200),
  Timestamp3 = maxwell_store_test_utils:get_current_timestamp(),

  Result = maxwell_store_topic_owner:seek_max_offset_until(Topic, Timestamp0),
  Result2 = maxwell_store_topic_owner:seek_max_offset_until(Topic, Timestamp1),
  Result3 = maxwell_store_topic_owner:seek_max_offset_until(Topic, Timestamp2),
  Result4 = maxwell_store_topic_owner:seek_max_offset_until(Topic, Timestamp3),
  [
    ?_assertMatch(undefined, Result),
    ?_assertMatch({ok, 0}, Result2),
    ?_assertMatch({ok, 1}, Result3),
    ?_assertMatch({ok, 2}, Result4)
  ].