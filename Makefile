.PHONY : default compile release auto test clean

REBAR=rebar3

default: auto

compile:
	${REBAR} get-deps
	${REBAR} compile

release: compile
	${REBAR} release

auto: release
	${REBAR} auto

test:
	${REBAR} eunit

clean:
	${REBAR} clean
