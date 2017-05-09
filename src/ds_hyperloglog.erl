%% -*- coding: utf-8 -*-
-module(ds_hyperloglog).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% This aims to be a faithful implementation of HyperLogLog.
%% See the following paper for a description of the algorithm:
%% http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

-export([ new/1
        , add/2
        , add_hash/2
        , join/2
        , estimate/1
        , error_est/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([ print_table/0 ]). % For manual use when test-compiled
-endif.

-include("config.hrl").
-include("random.hrl").

-record(hyperloglog,
        { b      % bit width of the number of registers
        , m      % number of registers, m = 2^b
        , dw     % data bit width, dw = 32 - b
        , dmask  % data bitmask, 2^dw - 1
        , regs   % tuple of integers, size(regs) = m
        }).

%% Initialize a HyperLogLog cardinality estimator.
new(false) -> undefined;
new(B) when is_integer(B), B >= 4, B =< 16 ->
    M = 1 bsl B,
    Dw = 32 - B,
    Dmask = 1 bsl Dw - 1,
    Regs = erlang:make_tuple(M, 0),
    #hyperloglog{b = B, m = M, dw = Dw, dmask = Dmask, regs = Regs};
new(T) ->
    error(badarg, [T]).

%% Add a new term to the HyperLogLog cardinality estimator.
add(_T, undefined) -> undefined;
add(T, HyperLogLog) ->
    Hash = ds_utils:hash(T),
    add_hash(Hash, HyperLogLog).

%% The HyperLogLog algorithm requires a hash that maps to 32-bit words.
%% This function is provided as an entry point in case you have already
%% hashed your terms with a suitable function and want to spare another
%% hash computation. In this case, please make sure to use a suitable
%% hash function (ds_utils:hash/1 is strongly recommended)!
%% In case of doubt, just use add/2 above.
add_hash(_Hash, undefined) -> undefined;
add_hash(Hash, #hyperloglog{b = B, dw = Dw, dmask = Dmask,
                            regs = Regs0} = HyperLogLog) ->
    Index = Hash bsr Dw + 1,
    Data = Hash band Dmask,
    Sigma = sigma(Data) - B, % leading zeros in Data only, not all 32 bits
    Regs = setelement(Index, Regs0, max(Sigma, element(Index, Regs0))),
    HyperLogLog#hyperloglog{regs = Regs}.

%% Two instances may be joined only if they are of the same parameter
join(undefined, HLL) -> HLL;
join(HLL, undefined) -> HLL;
join(#hyperloglog{b = B, regs = Regs1} = HLL,
     #hyperloglog{b = B, regs = Regs2}) ->
    RegsZL = lists:zip(tuple_to_list(Regs1), tuple_to_list(Regs2)),
    Regs = list_to_tuple([max(R1, R2) || {R1, R2} <- RegsZL]),
    HLL#hyperloglog{regs = Regs};
join(HLL1, HLL2) ->
    error(badarg, [HLL1, HLL2]).

estimate(undefined) -> 0;
estimate(#hyperloglog{m = M, regs = Regs} = HyperLogLog) ->
    E = raw_e(HyperLogLog),
    if E =< 5 * M / 2 ->
        case length([R || R <- tuple_to_list(Regs), R =:= 0]) of
            0 -> E;
            V -> M * math:log(M / V)
        end;
       E =< (1 bsl 32) / 30 ->
            E;
       true ->
            -(1 bsl 32) * math:log(1 - E / (1 bsl 32))
    end.

%% Estimation of typical relative error, plus/minus
error_est(undefined) -> 0;
error_est(#hyperloglog{m = M}) ->
    1.04 / math:sqrt(M).

%% Raw cardinality estimate formula
raw_e(#hyperloglog{b = B, m = M, regs = Regs}) ->
    Msquare = 1 bsl 2 * B,
    HarmonicSum = lists:sum([math:pow(2, -R) || R <- tuple_to_list(Regs)]),
    alpha(M) * Msquare / HarmonicSum.


alpha(16) -> 0.673;
alpha(32) -> 0.697;
alpha(64) -> 0.709;
alpha(M)  -> 0.7213 / (1 + 1.079 / M).


%% sigma(N)
%%
%% Return the number of leading zeros in the 32-bit binary representation of N.
%% For highest performance, this is arranged as a binary decision tree with the
%% higher branches first (the more leading zeros the less frequent the branch).
%%
%% The decision tree is constructed according to this table. The functions
%% s_X are named after the bit number X based on which they branch.
%%
%% | bit | lvl 1 | lvl 2 | lvl 3 | lvl 4 | lvl 5 |
%% |-----+-------+-------+-------+-------+-------|
%% |   0 |       |       |       |       |       |
%% |   1 |       |       |       |       | s_1   |
%% |   2 |       |       |       | s_2   |       |
%% |   3 |       |       |       |       | s_3   |
%% |   4 |       |       | s_4   |       |       |
%% |   5 |       |       |       |       | s_5   |
%% |   6 |       |       |       | s_6   |       |
%% |   7 |       |       |       |       | s_7   |
%% |   8 |       | s_8   |       |       |       |
%% |   9 |       |       |       |       | s_9   |
%% |  10 |       |       |       | s_10  |       |
%% |  11 |       |       |       |       | s_11  |
%% |  12 |       |       | s_12  |       |       |
%% |  13 |       |       |       |       | s_13  |
%% |  14 |       |       |       | s_14  |       |
%% |  15 |       |       |       |       | s_15  |
%% |  16 | sigma |       |       |       |       |
%% |  17 |       |       |       |       | s_17  |
%% |  18 |       |       |       | s_18  |       |
%% |  19 |       |       |       |       | s_19  |
%% |  20 |       |       | s_20  |       |       |
%% |  21 |       |       |       |       | s_21  |
%% |  22 |       |       |       | s_22  |       |
%% |  23 |       |       |       |       | s_23  |
%% |  24 |       | s_24  |       |       |       |
%% |  25 |       |       |       |       | s_25  |
%% |  26 |       |       |       | s_26  |       |
%% |  27 |       |       |       |       | s_27  |
%% |  28 |       |       | s_28  |       |       |
%% |  29 |       |       |       |       | s_29  |
%% |  30 |       |       |       | s_30  |       |
%% |  31 |       |       |       |       | s_31  |

%% Comment this out to use the naive implementation:
-define(SIGMA_TREE, sigma_tree).

-ifdef(SIGMA_TREE).
%% level 1
sigma(N) when N >= 1 bsl 16 -> s_24(N); sigma(N) -> s_8(N).

%% level 2
s_8 (N) when N >= 1 bsl  8 -> s_12(N); s_8 (N) -> s_4 (N).
s_24(N) when N >= 1 bsl 24 -> s_28(N); s_24(N) -> s_20(N).

%% level 3
s_4 (N) when N >= 1 bsl  4 -> s_6 (N); s_4 (N) -> s_2 (N).
s_12(N) when N >= 1 bsl 12 -> s_14(N); s_12(N) -> s_10(N).
s_20(N) when N >= 1 bsl 20 -> s_22(N); s_20(N) -> s_18(N).
s_28(N) when N >= 1 bsl 28 -> s_30(N); s_28(N) -> s_26(N).

%% level 4
s_2 (N) when N >= 1 bsl  2 -> s_3 (N); s_2 (N) -> s_1 (N).
s_6 (N) when N >= 1 bsl  6 -> s_7 (N); s_6 (N) -> s_5 (N).
s_10(N) when N >= 1 bsl 10 -> s_11(N); s_10(N) -> s_9 (N).
s_14(N) when N >= 1 bsl 14 -> s_15(N); s_14(N) -> s_13(N).
s_18(N) when N >= 1 bsl 18 -> s_19(N); s_18(N) -> s_17(N).
s_22(N) when N >= 1 bsl 22 -> s_23(N); s_22(N) -> s_21(N).
s_26(N) when N >= 1 bsl 26 -> s_27(N); s_26(N) -> s_25(N).
s_30(N) when N >= 1 bsl 30 -> s_31(N); s_30(N) -> s_29(N).

%% level 5
s_1 (0)                    -> 33;
s_1 (N) when N >= 1 bsl  1 -> 31; s_1 (_N) -> 32.
s_3 (N) when N >= 1 bsl  3 -> 29; s_3 (_N) -> 30.
s_5 (N) when N >= 1 bsl  5 -> 27; s_5 (_N) -> 28.
s_7 (N) when N >= 1 bsl  7 -> 25; s_7 (_N) -> 26.
s_9 (N) when N >= 1 bsl  9 -> 23; s_9 (_N) -> 24.
s_11(N) when N >= 1 bsl 11 -> 21; s_11(_N) -> 22.
s_13(N) when N >= 1 bsl 13 -> 19; s_13(_N) -> 20.
s_15(N) when N >= 1 bsl 15 -> 17; s_15(_N) -> 18.
s_17(N) when N >= 1 bsl 17 -> 15; s_17(_N) -> 16.
s_19(N) when N >= 1 bsl 19 -> 13; s_19(_N) -> 14.
s_21(N) when N >= 1 bsl 21 -> 11; s_21(_N) -> 12.
s_23(N) when N >= 1 bsl 23 ->  9; s_23(_N) -> 10.
s_25(N) when N >= 1 bsl 25 ->  7; s_25(_N) ->  8.
s_27(N) when N >= 1 bsl 27 ->  5; s_27(_N) ->  6.
s_29(N) when N >= 1 bsl 29 ->  3; s_29(_N) ->  4.
s_31(N) when N >= 1 bsl 31 ->  1; s_31(_N) ->  2.

-else.
%% For comparison, here is a simple descending-ladder implementation.

sigma(N) when N >= 1 bsl 31 ->  1;
sigma(N) when N >= 1 bsl 30 ->  2;
sigma(N) when N >= 1 bsl 29 ->  3;
sigma(N) when N >= 1 bsl 28 ->  4;
sigma(N) when N >= 1 bsl 27 ->  5;
sigma(N) when N >= 1 bsl 26 ->  6;
sigma(N) when N >= 1 bsl 25 ->  7;
sigma(N) when N >= 1 bsl 24 ->  8;
sigma(N) when N >= 1 bsl 23 ->  9;
sigma(N) when N >= 1 bsl 22 -> 10;
sigma(N) when N >= 1 bsl 21 -> 11;
sigma(N) when N >= 1 bsl 20 -> 12;
sigma(N) when N >= 1 bsl 19 -> 13;
sigma(N) when N >= 1 bsl 18 -> 14;
sigma(N) when N >= 1 bsl 17 -> 15;
sigma(N) when N >= 1 bsl 16 -> 16;
sigma(N) when N >= 1 bsl 15 -> 17;
sigma(N) when N >= 1 bsl 14 -> 18;
sigma(N) when N >= 1 bsl 13 -> 19;
sigma(N) when N >= 1 bsl 12 -> 20;
sigma(N) when N >= 1 bsl 11 -> 21;
sigma(N) when N >= 1 bsl 10 -> 22;
sigma(N) when N >= 1 bsl  9 -> 23;
sigma(N) when N >= 1 bsl  8 -> 24;
sigma(N) when N >= 1 bsl  7 -> 25;
sigma(N) when N >= 1 bsl  6 -> 26;
sigma(N) when N >= 1 bsl  5 -> 27;
sigma(N) when N >= 1 bsl  4 -> 28;
sigma(N) when N >= 1 bsl  3 -> 29;
sigma(N) when N >= 1 bsl  2 -> 30;
sigma(N) when N >= 1 bsl  1 -> 31;
sigma(N) when N >= 1 bsl  0 -> 32;
sigma(0)                    -> 33.

-endif.


%% Tests
-ifdef(TEST).

new_test() ->
    ?assertError(badarg, new(3)),
    ?assertError(badarg, new(17)),
    ?assertError(badarg, new(nan)),
    ?assertEqual(#hyperloglog{b=4, m=16, dw=28, dmask=1 bsl 28 - 1,
                              regs=erlang:make_tuple(16, 0)},
                 new(4)),
    ?assertEqual(#hyperloglog{b=5, m=32, dw=27, dmask=1 bsl 27 - 1,
                              regs=erlang:make_tuple(32, 0)},
                 new(5)).

sigma_test() ->
    LeadingZerosF =
        fun(N) ->
            BinStr = lists:flatten(io_lib:format("~32.2.0B", [N])),
            1 + length(lists:takewhile(fun($0) -> true;
                                          ($1) -> false
                                       end, BinStr))
        end,
    ?RNDINIT,
    sigma_test_loop(LeadingZerosF, 100000).

sigma_test_loop(_F, 0) -> ok;
sigma_test_loop(VerifF, Count) ->
    N = ?RNDMOD:uniform(1 bsl 32) - 1,
    ?assertEqual(VerifF(N), sigma(N)),
    sigma_test_loop(VerifF, Count-1).

hyperloglog_test() ->
    Count = 10000,
    HLL = lists:foldl(fun add/2, new(9),
                      [test_value(N) || N <- lists:seq(1, Count)]),
    AbsError = abs(Count - estimate(HLL)) / Count,
    ?assert(AbsError =< error_est(HLL)).

duplicates_test() ->
    Count = 25000,
    Unique = 600,
    HLL = lists:foldl(fun add/2, new(9),
                      [test_value(N rem Unique) || N <- lists:seq(1, Count)]),
    AbsError = abs(Unique - estimate(HLL)) / Count,
    ?assert(AbsError =< error_est(HLL)).

join_test() ->
    %% Two estimators fed with halfway overlapping sets, then joined
    %% results in the union set being counted once and only once.
    Start1 = 1,    End1 = 10000,
    Start2 = 5001, End2 = 15000,
    HLL1 = lists:foldl(fun add/2, new(9),
                       [test_value(N) || N <- lists:seq(Start1, End1)]),
    HLL2 = lists:foldl(fun add/2, new(9),
                       [test_value(N) || N <- lists:seq(Start2, End2)]),
    HLL = join(HLL1, HLL2),
    AbsError = abs(End2 - estimate(HLL)) / End2,
    ?assert(AbsError =< error_est(HLL)).

hash_quality_test() ->
    %% Make sure that the hash we use by default is good enough.
    N = 500000,
    HLL = hash_quality_test_feeder_loop(new(9), N),
    Estimate = estimate(HLL),
    ErrorEst = error_est(HLL),
    %% N.B.: estimated error is sometimes too small, hence the factor of 1.5
    LowerErrorBound = Estimate * (1.0 - 1.5 * ErrorEst),
    UpperErrorBound = Estimate * (1.0 + 1.5 * ErrorEst),
    ?assert(N >= LowerErrorBound),
    ?assert(N =< UpperErrorBound).

hash_quality_test_feeder_loop(HLL, 0) -> HLL;
hash_quality_test_feeder_loop(HLL, N) ->
   hash_quality_test_feeder_loop(add(erlang:make_ref(), HLL), N-1).
   %% NB. This fails the test:
   %% Hash = erlang:phash2(erlang:make_ref(), 1 bsl 32),
   %% hash_quality_test_feeder_loop(add_hash(Hash, HLL), N-1).

%% Print a nice table of hyperloglog estimator performance.
%% This is not part of the EUnit tests, must be run manually.
print_table() ->
    HLLs0 = [new(B) || B <- lists:seq(4, 16)],
    AddF = fun(E, HLLs) -> [add(E, HLL) || HLL <- HLLs] end,

    io:format(user, "~n       m:", []),
    [io:format(user, " ~7B", [M]) || #hyperloglog{m=M} <- HLLs0],
    io:format(user, "~n   e_est:", []),
    [io:format(user, " ~7.5f", [error_est(HLL)]) || HLL <- HLLs0],
    io:format(user, "~n    count / e_real", []),

    E12 = [10, 12, 15, 18, 22, 27, 33, 39, 47, 56, 68, 82],
    Counts = lists:flatten([lists:seq(1, 9),
                            [[E * round(math:pow(10, Dek)) || E <- E12] ||
                                Dek <- lists:seq(0, 7)]]),
    F = fun(Count, {HLLsA0, Count0}) ->
            HLLsA = test_feed_hlls(AddF, HLLsA0, Count0, Count),
            print_error(HLLsA, Count),
            {HLLsA, Count}
        end,
    lists:foldl(F, {HLLs0, 0}, Counts),
    io:format(user, "~n", []).

test_feed_hlls(_AddF, HLLs0, EndCount, EndCount) -> HLLs0;
test_feed_hlls(AddF, HLLs0, Count, EndCount) ->
    HLLs = AddF(test_value(Count+1), HLLs0),
    test_feed_hlls(AddF, HLLs, Count+1, EndCount).

print_error(HLLs, Count) when is_list(HLLs) ->
    io:format(user, "~n~9B", [Count]),
    [print_error(HLL, Count) || HLL <- HLLs];
print_error(HLL, Count) ->
    Estimate = round(estimate(HLL)),
    RealError = (Estimate - Count) / Count,
    io:format(user, " ~7.4f", [RealError]).

test_value(N) ->
    case N rem 5 of
        0 -> {value, N};
        1 -> [list, with, N];
        2 -> integer_to_list(N);
        3 -> term_to_binary(N * N);
        4 -> float(N);
        5 -> N
    end.

-endif.
