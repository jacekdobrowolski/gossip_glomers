#!/bin/bash

cwd=$(pwd)
go install .
cd ~/maelstrom
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10
cd $cwd