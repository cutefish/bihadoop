#!/bin/bash

hadoop jar build/bench/Bench.jar bench.pagerank.PagerankMap2 /data/pagerank/soc-LiveJournal1/soc-LiveJournal1.txt /data/pagerank/soc-LiveJournal1/rank &>~/map2
hadoop jar build/bench/Bench.jar bench.pagerank.PagerankNaive /data/pagerank/soc-LiveJournal1/soc-LiveJournal1.txt /data/pagerank/soc-LiveJournal1/rank &>~/naive
