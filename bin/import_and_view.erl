#!/usr/bin/env escript
%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jun 2018 8:25 AM
%%%-------------------------------------------------------------------
-module(import_demo_data).

main([Topic, Count]) ->
  try
    ok = start_maxwell_store(),
    import_data(
      list_to_binary(Topic),
      list_to_integer(Count)
    )
  catch
    Error ->
      log("Error occured: ~p", [Error]),
      usage()
  end;
main(["view"]) ->
  ok = start_maxwell_store(),
  view_data();
main(Any) ->
  log("Unknown args: ~p", [Any]),
  usage().

start_maxwell_store() ->
  add_code_path(),
  add_env_vars(),
  case application:ensure_all_started(maxwell_store) of
    {ok, _} -> ok;
    {error, {already_started, _}} -> ok;
    Error -> Error
  end.

add_code_path() ->
  LibDir = detect_lib_dir(),
  log("Detected lib dir: ~p", [LibDir]),
  {ok, Filenames} = file:list_dir(LibDir),
  log("Detected libs: ~p", [Filenames]),
  lists:foreach(
    fun(Lib) ->
      true = code:add_pathz(LibDir ++ "/" ++ Lib ++ "/ebin")
    end, Filenames
  ).

detect_lib_dir() ->
  ScriptDir = filename:absname(filename:dirname(escript:script_name())),
  LibDir0 = ScriptDir ++ "/../_build/default/lib",
  LibDir1 = ScriptDir ++ "/../..",
  case filelib:is_dir(LibDir0 ++ "/maxwell_store") of
    true -> LibDir0;
    false ->
      case filelib:is_dir(LibDir1 ++ "/maxwell_store") of
        true -> LibDir1;
        false -> erlang:error({no_lib_dir, LibDir0, LibDir1})
      end
  end.

add_env_vars() ->
  DataDir = detect_data_dir(),
  log("Detected data dir: ~p", [DataDir]),
  application:set_env(maxwell_store, data_dir, DataDir),
  application:set_env(maxwell_store, retention_age, 30),
  application:set_env(maxwell_store, clean_interval, 5).

detect_data_dir() ->
  ScriptDir = filename:absname(filename:dirname(escript:script_name())),
  DataDir0 = ScriptDir ++ "/../data",
  DataDir1 = ScriptDir ++ "/../../../../../data/maxwell_store/",
  case filelib:is_file(DataDir0 ++ "/LOCK") of
    true -> DataDir0;
    false ->
      case filelib:is_file(DataDir1 ++ "/LOCK") of
        true -> DataDir1;
        false -> erlang:error({no_data_dir, DataDir0, DataDir1})
      end
  end.

import_data(Topic, Count) ->
  import_data0(Topic, Count-1, Count - 1).

import_data0(Topic, 0, Max) ->
  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<Max/integer>>]);
import_data0(Topic, N, Max) ->
  {ok, _} = maxwell_store_topic_owner:ensure_started(Topic),
  ok = maxwell_store_topic_owner:put_values(Topic, [<<(Max - N)/integer>>]),
  timer:sleep(10),
  import_data0(Topic, N - 1, Max).

view_data() ->
  {ok, DbRef} = maxwell_store_db_owner:get_ref(),
  maxwell_store_db:foreach(DbRef,
    fun(T, E) ->
      log("view: ~p, ~p", [T, E])
    end
  ).

usage() ->
  log("usage: import_and_view topic count(>0)"),
  halt(1).

log(Format) ->
  log(Format, []).

log(Format, Args) ->
  io:format(Format ++ "\n", Args).