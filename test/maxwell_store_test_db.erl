%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jun 2018 5:48 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_test_db).
-include_lib("eunit/include/eunit.hrl").

-export([
  setup/0,
  teardown/1,
  create_topic_test_/0,
  delete_topic_test_/0,
  seek_first_topic_test_/0,
  seek_last_topic_test_/0,
  seek_prev_topic_test_/0,
  seek_next_topic_test_/0,
  put_test_/0,
  put_batch_test_/0,
  delete_test_/0,
  delete_batch_test_/0,
  delete_to_test_/0,
  delete_range_test_/0,
  get_test_/0,
  get_from_test_/0,
  seek_offset_test_/0,
  seek_min_offset_since_test_/0,
  seek_max_offset_until_test_/0
]).

setup() ->
  {ok, DbRef} = maxwell_store_test_utils:setup(),
  DbRef.

teardown(DbRef) ->
  maxwell_store_test_utils:teardown(DbRef).

create_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun create_topic/1}.

delete_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun delete_topic/1}.

seek_first_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_first_topic/1}.

seek_last_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_last_topic/1}.

seek_prev_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_prev_topic/1}.

seek_next_topic_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_next_topic/1}.

put_test_() ->
  {setup, fun setup/0, fun teardown/1, fun put/1}.

put_batch_test_() ->
  {setup, fun setup/0, fun teardown/1, fun put_batch/1}.

delete_test_() ->
  {setup, fun setup/0, fun teardown/1, fun delete/1}.

delete_batch_test_() ->
  {setup, fun setup/0, fun teardown/1, fun delete_batch/1}.

delete_to_test_() ->
  {setup, fun setup/0, fun teardown/1, fun delete_to/1}.

delete_range_test_() ->
  {setup, fun setup/0, fun teardown/1, fun delete_range/1}.

get_test_() ->
  {setup, fun setup/0, fun teardown/1, fun get/1}.

get_from_test_() ->
  {setup, fun setup/0, fun teardown/1, fun get_from/1}.

seek_offset_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_offset/1}.

seek_min_offset_since_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_min_offset_since/1}.

seek_max_offset_until_test_() ->
  {setup, fun setup/0, fun teardown/1, fun seek_max_offset_until/1}.

create_topic(DbRef) ->
  Result = maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  Result2 = maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  Result3 = maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  [
    ?_assertMatch({ok, 1024}, Result),
    ?_assertMatch({ok, 1025}, Result2),
    ?_assertMatch({ok, 1025}, Result3)
  ].

delete_topic(DbRef) ->
  Result = maxwell_store_db:delete_topic(DbRef, <<"Topic1">>),
  maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  Result2 = maxwell_store_db:delete_topic(DbRef, <<"Topic1">>),
  Result3 = maxwell_store_db:delete_topic(DbRef, <<"Topic1">>),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch(ok, Result3)
  ].

seek_first_topic(DbRef) -> 
  Result = maxwell_store_db:seek_first_topic(DbRef),
  maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  Result2 = maxwell_store_db:seek_first_topic(DbRef),
  maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  Result3 = maxwell_store_db:seek_first_topic(DbRef),
  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch({ok, <<"Topic1">>, 1024}, Result2),
    ?_assertMatch({ok, <<"Topic1">>, 1024}, Result3)
  ].

seek_last_topic(DbRef) -> 
  Result = maxwell_store_db:seek_last_topic(DbRef),
  maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  Result2 = maxwell_store_db:seek_last_topic(DbRef),
  maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  Result3 = maxwell_store_db:seek_last_topic(DbRef),
  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch({ok, <<"Topic1">>, 1024}, Result2),
    ?_assertMatch({ok, <<"Topic2">>, 1025}, Result3)
  ].

seek_prev_topic(DbRef) -> 
  Result = maxwell_store_db:seek_prev_topic(DbRef, 1024),
  maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  {ok, _, _} = maxwell_store_db:seek_last_topic(DbRef),
  Result2 = maxwell_store_db:seek_prev_topic(DbRef, 1024),
  maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  Result3 = maxwell_store_db:seek_prev_topic(DbRef, 1024),
  Result4 = maxwell_store_db:seek_prev_topic(DbRef, 1025),
  Result5 = maxwell_store_db:seek_prev_topic(DbRef, 1026),
  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch(not_found, Result2),
    ?_assertMatch(not_found, Result3),
    ?_assertMatch({ok, <<"Topic1">>, 1024}, Result4),
    ?_assertMatch({ok, <<"Topic2">>, 1025}, Result5)
  ].

seek_next_topic(DbRef) -> 
  Result = maxwell_store_db:seek_next_topic(DbRef, 1024),
  maxwell_store_db:create_topic(DbRef, <<"Topic1">>),
  Result2 = maxwell_store_db:seek_next_topic(DbRef, 1024),
  Result3 = maxwell_store_db:seek_next_topic(DbRef, 1023),
  maxwell_store_db:create_topic(DbRef, <<"Topic2">>),
  Result4 = maxwell_store_db:seek_next_topic(DbRef, 1024),
  Result5 = maxwell_store_db:seek_next_topic(DbRef, 1025),
  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch(not_found, Result2),
    ?_assertMatch({ok, <<"Topic1">>, 1024}, Result3),
    ?_assertMatch({ok, <<"Topic2">>, 1025}, Result4),
    ?_assertMatch(not_found, Result5)
  ].


put(DbRef) ->
  Topic = 10000,
  Topic4 = 20000,

  Offset_1 = 1024,
  Offset_2 = 1025,
  Offset_3 = 1026,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,
  Offset4_4 = -1,
  Offset4_5 = 18446744073709552000,

  Result = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  Result2 = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  Result3 = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  Result4 = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  Result5 = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  Result6 = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),
  Result7 = (catch maxwell_store_db:put(DbRef, Topic4, Offset4_4, <<"Value4_4">>)),
  Result8 = (catch maxwell_store_db:put(DbRef, Topic4, Offset4_5, <<"Value4_5">>)),

  maxwell_store_test_utils:show(DbRef),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch(ok, Result3),
    ?_assertMatch(ok, Result4),
    ?_assertMatch(ok, Result5),
    ?_assertMatch(ok, Result6),
    ?_assertMatch({'EXIT', {{out_of_range, 0, 18446744073709551614}, _}}, Result7),
    ?_assertMatch({'EXIT', {{out_of_range, 0, 18446744073709551614}, _}}, Result8)
  ].

put_batch(DbRef) ->
  Topic = 10000,
  Topic4 = 20000,

  Entries = [
    {1024, <<"Value_1">>},
    {1025, <<"Value_2">>},
    {1026, <<"Value_3">>}
  ],
  Entries2 = [
    {1024, <<"Value_1">>},
    {1025, <<"Value_2">>},
    {1026, <<"Value_3">>}
  ],
  Entries3 = [
    {1024, <<"Value_1">>},
    {-1, <<"Value_-1">>},
    {1025, <<"Value_2">>},
    {1026, <<"Value_3">>},
    {18446744073709552000, <<"Value_18446744073709551999">>}
  ],
  Result = maxwell_store_db:put_batch(DbRef, Topic, Entries),
  Result2 = maxwell_store_db:put_batch(DbRef, Topic4, Entries2),
  Result3 = (catch maxwell_store_db:put_batch(DbRef, Topic4, Entries3)),
  maxwell_store_test_utils:show(DbRef),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch({'EXIT', {{out_of_range, 0, 18446744073709551614}, _}}, Result3)
  ].

delete(DbRef) ->
  Topic = 10000,
  Offset_2 = 1025,
  Offset_3 = 1026,
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_1">>),
  Result = maxwell_store_db:delete(DbRef, Topic, Offset_2),
  Result2 = maxwell_store_db:delete(DbRef, Topic, Offset_3),
  maxwell_store_test_utils:show(DbRef),
  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2)
  ].

delete_batch(DbRef) ->
  Topic = 10000,
  Offset_2 = 1025,
  Offset_3 = 1026,
  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_1">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_1">>),
  Result = maxwell_store_db:delete_batch(DbRef, Topic, Offset_2, Offset_3),
  maxwell_store_test_utils:show(DbRef),
  [
    ?_assertMatch(ok, Result)
  ].

delete_to(DbRef) ->
  Offset_1 = 1124,
  Offset_2 = 1126,
  Offset_3 = 1128,
  Offset_4 = 1132,
  Offset4_0 = 1023,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),
  {ok, Topic4} = maxwell_store_db:create_topic(DbRef, <<"topic_4">>),

  Result = (catch maxwell_store_db:delete_to(DbRef, Topic, Offset_1, 0)),
  Result2 = (catch maxwell_store_db:delete_to(DbRef, Topic, Offset_4, 1)),

  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_4, <<"Value_4">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),

  Result3 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),
  Result4 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_0, 4),

  Result5 = maxwell_store_db:delete_to(DbRef, Topic, Offset_1, 1),
  Result6 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),

  Result7 = maxwell_store_db:delete_to(DbRef, Topic, Offset_3, 1),
  Result8 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),

  Result9 = maxwell_store_db:delete_to(DbRef, Topic, Offset_4, 10),
  Result10 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),
  Result11 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_0, 4),

  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch({'EXIT', {{invalid_limit, _}, _}}, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch([
      {Offset_2, <<"Value_2">>, _}, {Offset_3, <<"Value_3">>, _}, {Offset_4, <<"Value_4">>, _}
    ], Result3),
    ?_assertMatch([
      {Offset4_1, <<"Value4_1">>, _}, {Offset4_2, <<"Value4_2">>, _}, {Offset4_3, <<"Value4_3">>, _}
    ], Result4),
    ?_assertMatch(ok, Result5),
    ?_assertMatch([
      {Offset_2, <<"Value_2">>, _}, {Offset_3, <<"Value_3">>, _}, {Offset_4, <<"Value_4">>, _}
    ], Result6),
    ?_assertMatch(ok, Result7),
    ?_assertMatch([{Offset_3, <<"Value_3">>, _}, {Offset_4, <<"Value_4">>, _}], Result8),
    ?_assertMatch(ok, Result9),
    ?_assertMatch([], Result10),
    ?_assertMatch([
      {Offset4_1, <<"Value4_1">>, _}, {Offset4_2, <<"Value4_2">>, _}, {Offset4_3, <<"Value4_3">>, _}
    ], Result11)
  ].

delete_range(DbRef) ->
  Offset_0 = 1122,
  Offset_1 = 1124,
  Offset_2 = 1126,
  Offset_3 = 1128,
  Offset_4 = 1132,
  Offset4_0 = 1023,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),
  {ok, Topic4} = maxwell_store_db:create_topic(DbRef, <<"topic_4">>),

  Result = (catch maxwell_store_db:delete_range(DbRef, Topic, Offset_0, Offset_0)),
  Result2 = (catch maxwell_store_db:delete_range(DbRef, Topic, Offset_0, Offset4_3)),

  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),

  Result3 = maxwell_store_db:get_from(DbRef, Topic, Offset_0, 4),
  Result4 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_0, 4),

  Result5 = maxwell_store_db:delete_range(DbRef, Topic, Offset_1, Offset_1),
  Result6 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),

  Result7 = maxwell_store_db:delete_range(DbRef, Topic, Offset_3, Offset_4),
  Result8 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),

  Result9 = maxwell_store_db:delete_range(DbRef, Topic, Offset_3, 2000),
  Result10 = maxwell_store_db:get_from(DbRef, Topic, Offset_1, 4),
  Result11 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_0, 4),

  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch(ok, Result),
    ?_assertMatch(ok, Result2),
    ?_assertMatch([
      {Offset_1, <<"Value_1">>, _}, {Offset_2, <<"Value_2">>, _}, {Offset_3, <<"Value_3">>, _}
    ], Result3),
    ?_assertMatch([
      {Offset4_1, <<"Value4_1">>, _}, {Offset4_2, <<"Value4_2">>, _}, {Offset4_3, <<"Value4_3">>, _}
    ], Result4),
    ?_assertMatch(ok, Result5),
    ?_assertMatch([
      {Offset_1, <<"Value_1">>, _}, {Offset_2, <<"Value_2">>, _}, {Offset_3, <<"Value_3">>, _}
    ], Result6),
    ?_assertMatch(ok, Result7),
    ?_assertMatch([{Offset_1, <<"Value_1">>, _}, {Offset_2, <<"Value_2">>, _}], Result8),
    ?_assertMatch(ok, Result9),
    ?_assertMatch([{Offset_1, <<"Value_1">>, _}, {Offset_2, <<"Value_2">>, _}], Result10),
    ?_assertMatch([
      {Offset4_1, <<"Value4_1">>, _}, {Offset4_2, <<"Value4_2">>, _}, {Offset4_3, <<"Value4_3">>, _}
    ], Result11)
  ].

get(DbRef) ->
  Offset_1 = 1124,
  Offset_2 = 1126,
  Offset_3 = 1128,
  Offset_4 = 1132,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),
  {ok, Topic4} = maxwell_store_db:create_topic(DbRef, <<"topic_4">>),

  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_4, <<"Value_4">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),

  Result2 = maxwell_store_db:get(DbRef, Topic, Offset_2),
  Result3 = maxwell_store_db:get(DbRef, Topic, Offset_3),
  Result4 = maxwell_store_db:get(DbRef, Topic4, Offset4_1),
  
  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch(not_found, Result2),
    ?_assertMatch({ok, {<<"Value_3">>, _}}, Result3),
    ?_assertMatch({ok, {<<"Value4_1">>, _}}, Result4)
  ].

get_from(DbRef) ->
  Offset_1 = 1124,
  Offset_2 = 1126,
  Offset_3 = 1128,
  Offset_4 = 1132,
  Offset4_0 = 1023,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),
  {ok, Topic4} = maxwell_store_db:create_topic(DbRef, <<"topic_4">>),

  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_4, <<"Value_4">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),

  Result = (catch maxwell_store_db:get_from(DbRef, Topic4, Offset4_1, 0)),
  Result2 = maxwell_store_db:get_from(DbRef, Topic, Offset_2, 1),
  Result3 = maxwell_store_db:get_from(DbRef, Topic, Offset_2, 4),
  Result4 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_1, 1),
  Result5 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_1, 2),
  Result6 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_1, 3),
  Result7 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_1, 4),
  Result8 = maxwell_store_db:get_from(DbRef, Topic4, Offset4_0, 2),

  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch({'EXIT', {{invalid_limit, 0}, _}}, Result),
    ?_assertMatch([{1128, <<"Value_3">>, _}], Result2),
    ?_assertMatch([{1128, <<"Value_3">>, _}, {1132, <<"Value_4">>, _}], Result3),
    ?_assertMatch([{1024, <<"Value4_1">>, _}], Result4),
    ?_assertMatch([{1024, <<"Value4_1">>, _}, {1025, <<"Value4_2">>, _}], Result5),
    ?_assertMatch([{1024, <<"Value4_1">>, _}, {1025, <<"Value4_2">>, _}, {1026, <<"Value4_3">>, _}], Result6),
    ?_assertMatch([{1024, <<"Value4_1">>, _}, {1025, <<"Value4_2">>, _}, {1026, <<"Value4_3">>, _}], Result7),
    ?_assertMatch([{1024, <<"Value4_1">>, _}, {1025, <<"Value4_2">>, _}], Result8)
  ].

seek_offset(DbRef) ->
  Offset_1 = 1024,
  Offset_2 = 1025,
  Offset_3 = 1026,
  Offset4_1 = 1024,
  Offset4_2 = 1025,
  Offset4_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),
  {ok, Topic4} = maxwell_store_db:create_topic(DbRef, <<"topic_4">>),
  Result = maxwell_store_db:seek_min_offset(DbRef, Topic),
  Result2 = maxwell_store_db:seek_max_offset(DbRef, Topic),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_1, <<"Value4_1">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_2, <<"Value4_2">>),
  ok = maxwell_store_db:put(DbRef, Topic4, Offset4_3, <<"Value4_3">>),
  Result3 = maxwell_store_db:seek_min_offset(DbRef, Topic),
  Result4 = maxwell_store_db:seek_max_offset(DbRef, Topic),
  Result5 = maxwell_store_db:seek_min_offset(DbRef, Topic4),
  Result6 = maxwell_store_db:seek_max_offset(DbRef, Topic4),
  maxwell_store_test_utils:show(DbRef),
  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch(not_found, Result2),
    ?_assertMatch({ok, 1024}, Result3),
    ?_assertMatch({ok, 1026}, Result4),
    ?_assertMatch({ok, 1024}, Result5),
    ?_assertMatch({ok, 1026}, Result6)
  ].

seek_min_offset_since(DbRef) ->
  Offset_1 = 1024,
  Offset_2 = 1025,
  Offset_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),

  Timestamp0 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  timer:sleep(1000),
  Timestamp1 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  timer:sleep(1000),
  Timestamp2 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  timer:sleep(1000),
  Timestamp3 = maxwell_store_test_utils:get_current_timestamp(),

  Result = maxwell_store_db:seek_min_offset_since(DbRef, Topic, Timestamp0),
  Result2 = maxwell_store_db:seek_min_offset_since(DbRef, Topic, Timestamp1),
  Result3 = maxwell_store_db:seek_min_offset_since(DbRef, Topic, Timestamp2),
  Result4 = maxwell_store_db:seek_min_offset_since(DbRef, Topic, Timestamp3),

  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch({ok, 1024}, Result),
    ?_assertMatch({ok, 1025}, Result2),
    ?_assertMatch({ok, 1026}, Result3),
    ?_assertMatch(not_found, Result4)
  ].

seek_max_offset_until(DbRef) ->
  Offset_1 = 1024,
  Offset_2 = 1025,
  Offset_3 = 1026,

  {ok, Topic} = maxwell_store_db:create_topic(DbRef, <<"topic_1">>),

  Timestamp0 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_1, <<"Value_1">>),
  timer:sleep(1000),
  Timestamp1 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_2, <<"Value_2">>),
  timer:sleep(1000),
  Timestamp2 = maxwell_store_test_utils:get_current_timestamp(),
  timer:sleep(1000),
  ok = maxwell_store_db:put(DbRef, Topic, Offset_3, <<"Value_3">>),
  timer:sleep(1000),
  Timestamp3 = maxwell_store_test_utils:get_current_timestamp(),

  Result = maxwell_store_db:seek_max_offset_until(DbRef, Topic, Timestamp0),
  Result2 = maxwell_store_db:seek_max_offset_until(DbRef, Topic, Timestamp1),
  Result3 = maxwell_store_db:seek_max_offset_until(DbRef, Topic, Timestamp2),
  Result4 = maxwell_store_db:seek_max_offset_until(DbRef, Topic, Timestamp3),

  maxwell_store_test_utils:show(DbRef),

  [
    ?_assertMatch(not_found, Result),
    ?_assertMatch({ok, 1024}, Result2),
    ?_assertMatch({ok, 1025}, Result3),
    ?_assertMatch({ok, 1026}, Result4)
  ].