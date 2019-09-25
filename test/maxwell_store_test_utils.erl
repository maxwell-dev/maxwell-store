%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jun 2018 5:59 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_test_utils).

%% API
-export([
  setup/0,
  teardown/1,
  show/1,
  get_current_timestamp/0
]).

setup() ->
  DataDir = "/Users/xuchaoqian/codebase/hongjia/maxwell_store/data/",
  os:cmd("rm -rf " ++ DataDir ++ "/*"),
  application:set_env(maxwell_store, data_dir, DataDir),
  application:set_env(maxwell_store, retention_age, 30),
  application:set_env(maxwell_store, clean_interval, 5),
  case application:ensure_all_started(maxwell_store) of
    {ok, _} -> {ok, open_db(DataDir)};
    {error, {already_started, _}} -> {ok, open_db(DataDir)};
    Error ->
      Error
  end.

teardown(DbRef) ->
  maxwell_store_db:close(DbRef),
  ok = application:stop(maxwell_store).

show(DbRef) ->
  error_logger:info_msg("{{{{{{{{{{{"),
  maxwell_store_db:foreach(DbRef,
    fun(T, E) ->
      error_logger:info_msg("put foreach: ~p, ~p", [T, E])
    end
  ),
  error_logger:info_msg("}}}}}}}}}}}").

get_current_timestamp() ->
  {MegaSec, Sec, MicroSec} = os:timestamp(),
  1000000000 * MegaSec + Sec * 1000 + erlang:trunc(MicroSec / 1000).

open_db(DataDir) -> 
  {ok, DbRef} = maxwell_store_db:open(DataDir, [
    {create_if_missing, true},
    {prefix_extractor, {fixed_prefix_transform, 4}},
    {max_open_files, -1},
    {use_fsync, false},
    {bytes_per_sync, 8388608},
    {table_cache_numshardbits, 6},
    {write_buffer_size, 268435456},
    {max_write_buffer_number, 4},
    {min_write_buffer_number_to_merge, 2},
    {target_file_size_base, 1073741824},
    {level0_slowdown_writes_trigger, 1024},
    {level0_stop_writes_trigger, 800},
    {compaction_style, universal},
    {max_background_compactions, 2},
    {max_background_flushes, 2},
    {total_threads, 2}
  ]),
  DbRef.