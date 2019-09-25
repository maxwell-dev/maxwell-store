%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Mar 2018 6:00 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_sup).
-behaviour(supervisor).

%% API
-export([
  start_link/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP_NAME, ?MODULE).
-define(SPEC(Module, Type), #{
  id => Module,
  start => {Module, start_link, []},
  restart => permanent,
  shutdown => infinity,
  type => Type,
  modules => [Module]}
).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
  supervisor:start_link({local, ?SUP_NAME}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 60},
  ChildSpecs = [
    ?SPEC(maxwell_store_proc_registry, worker),
    ?SPEC(maxwell_store_db_owner, worker),
    ?SPEC(maxwell_store_cleaner, worker),
    ?SPEC(maxwell_store_topic_owner_sup, supervisor)
  ],
  {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================