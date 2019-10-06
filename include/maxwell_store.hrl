%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Sep 2018 2:08 PM
%%%-------------------------------------------------------------------

-ifndef(maxwell_store).
-define(maxwell_store, true).

-define(MAXWELL_STORE_NOTIFY_CMD(TopicName, MaxOffset), {'$maxwell_store_notify', TopicName, MaxOffset}).

-endif.
