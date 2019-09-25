%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jun 2018 6:03 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_topic_owner_sup).
-behaviour(supervisor).

%% API
-export([
  start_link/0,
  start_child/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP_NAME, ?MODULE).
-define(SPEC(Module), #{
  id => Module,
  start => {Module, start_link, []},
  restart => temporary,
  shutdown => 100, % ms
  type => worker,
  modules => [Module]}
).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
  supervisor:start_link({local, ?SUP_NAME}, ?MODULE, []).

start_child(TopicName) ->
  supervisor:start_child(?SUP_NAME, [TopicName]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
  SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
  ChildSpecs = [?SPEC(maxwell_store_topic_owner)],
  {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================