%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jun 2018 6:13 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_config).

%% API
-export([
  get_data_dir/0,
  get_retention_age/0,
  get_clean_interval/0
]).

get_data_dir() ->
  {ok, DataDir} = application:get_env(maxwell_store, data_dir),
  string:strip(DataDir, right, $/).

get_retention_age() ->
  {ok, RetentionAge} = application:get_env(maxwell_store, retention_age),
  RetentionAge.

get_clean_interval() ->
  {ok, CheckInterval} = application:get_env(maxwell_store, clean_interval),
  CheckInterval.