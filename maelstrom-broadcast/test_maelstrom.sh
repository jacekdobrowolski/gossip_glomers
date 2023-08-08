#!/bin/bash

cwd=$(pwd)
go install .
cd ~/maelstrom || exit
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
cd "$cwd" || exit