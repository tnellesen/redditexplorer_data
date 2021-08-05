#!/bin/bash

Rscript database/1_extract_comment_data.r &>> logs/extract_comment_data.txt &
Rscript database/3_build_comment_database.r &>> logs/build_comment_database.txt &

while true
  do Rscript database/2_parse_comment_data.r &>> logs/parse_comment_data.txt
  sleep 15
done

