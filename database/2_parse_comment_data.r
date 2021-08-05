library(purrr)  
library(future)
library(data.table)
library(duckdb)

plan(multiprocess(workers = 2))

compressed_files <- list.files(
  '/data/drv2/Reddit/Raw',
  pattern = 'RC_',
  full.names = TRUE
) %>%
  keep(~grepl("\\.(bz2|xz|zst)$", .))

cols <- c(
  'archived', 'author', 'author_flair_css_class', 'author_flair_text', 'body', 'controversiality', 'created_utc', 
  'distinguished', 'downs', 'edited', 'gilded', 'id', 'link_id', 'name', 'parent_id', 'retrieved_on', 'score', 
  'score_hidden', 'subreddit', 'subreddit_id', 'ups', 'stickied', 'removal_reason', 'can_gild', 'approved_at_utc', 
  'can_mod_post', 'collapsed', 'collapsed_reason', 'is_submitter', 'permalink', 'subreddit_type', 'no_follow',
  'send_replies', 'author_flair_template_id', 'author_flair_background_color', 'author_flair_text_color', 'rte_mode',
  'subreddit_name_prefixed', 'author_created_utc', 'author_flair_richtext', 'author_flair_type', 'author_fullname', 
  'gildings_gid_1', 'gildings_gid_2', 'gildings_gid_3', 'author_patreon_flair', 'author_flair_richtext_0_a', 
  'author_flair_richtext_0_e', 'author_flair_richtext_0_u', 'author_flair_richtext_1_e', 'author_flair_richtext_1_t',
  'quarantined', 'all_awardings', 'gildings', 'locked', 'total_awards_received', 'steward_reports', 'associated_award',
  'awarders', 'author_premium', 'collapsed_because_crowd_control', 'author_cakeday', 'banned_at_utc', 'created_date', 'file'
)

# Database schema created via CLI:
# # CREATE TABLE comments(
# #   archived BOOLEAN,
# #   author VARCHAR,
# #   author_flair_css_class VARCHAR,
# #   author_flair_text VARCHAR,
# #   body VARCHAR,
# #   controversiality DOUBLE,
# #   created_utc TIMESTAMP,
# #   distinguished VARCHAR,
# #   downs DOUBLE,
# #   edited DOUBLE,
# #   gilded DOUBLE,
# #   id VARCHAR PRIMARY KEY,
# #   link_id VARCHAR,
# #   name VARCHAR,
# #   parent_id VARCHAR,
# #   retrieved_on DOUBLE,
# #   score DOUBLE,
# #   score_hidden BOOLEAN,
# #   subreddit VARCHAR,
# #   subreddit_id VARCHAR,
# #   ups DOUBLE,
# #   stickied BOOLEAN,
# #   removal_reason VARCHAR,
# #   can_gild BOOLEAN,
# #   approved_at_utc TIMESTAMP,
# #   can_mod_post BOOLEAN,
# #   collapsed BOOLEAN,
# #   collapsed_reason VARCHAR,
# #   is_submitter BOOLEAN,
# #   permalink VARCHAR,
# #   subreddit_type VARCHAR,
# #   no_follow BOOLEAN,
# #   send_replies BOOLEAN,
# #   author_flair_template_id VARCHAR,
# #   author_flair_background_color VARCHAR,
# #   author_flair_text_color VARCHAR,
# #   rte_mode VARCHAR,
# #   subreddit_name_prefixed VARCHAR,
# #   author_created_utc TIMESTAMP,
# #   author_flair_richtext VARCHAR,
# #   author_flair_type VARCHAR,
# #   author_fullname VARCHAR,
# #   gildings_gid_1 INTEGER,
# #   gildings_gid_2 INTEGER,
# #   gildings_gid_3 INTEGER,
# #   author_patreon_flair VARCHAR,
# #   author_flair_richtext_0_a VARCHAR,
# #   author_flair_richtext_0_e VARCHAR,
# #   author_flair_richtext_0_u VARCHAR,
# #   author_flair_richtext_1_e VARCHAR,
# #   author_flair_richtext_1_t VARCHAR,
# #   quarantined BOOLEAN,
# #   all_awardings VARCHAR,
# #   gildings VARCHAR,
# #   locked BOOLEAN,
# #   total_awards_received INTEGER,
# #   steward_reports VARCHAR,
# #   associated_award VARCHAR,
# #   awarders VARCHAR,
# #   author_premium BOOLEAN,
# #   collapsed_because_crowd_control BOOLEAN,
# #   author_cakeday BOOLEAN,
# #   banned_at_utc TIMESTAMP,
# #   created_date DATE,
# #   file VARCHAR
# # );

uncompressed_files <- list()

for (i in rev(1:length(compressed_files))) {
  f <- compressed_files[[i]]
  
  uncompressed_files[[i]] <- list.files(paste0("/mnt/zfs/Reddit/Uncompressed/", substr(basename(f), 4, 10)), full.names = TRUE, pattern = "__..$")
  if (length(uncompressed_files[[i]]) > 0) break 
}

delete_if_finished <- function(f) {
  all_files <- dirname(f) %>%
    list.files(full.names = TRUE) 
  edit_times <- all_files %>%
    file.info %>%
    .$mtime
  most_recent_edit <- edit_times %>%
    .[which.max(.)]
  print(data.table(all_files = all_files, edit_times = edit_times))
  if (as.double.difftime(Sys.time()-most_recent_edit, units = 'secs') > 15) file.remove(f) else file.rename(f, paste0(f, '2')) 
}


for (j in seq_len(length(uncompressed_files[[i]]))) {
  f <- uncompressed_files[[i]][j]
  print(f)
  
  # Delete an ndjson chunk if csv exists and ndjson chunks are not currently being extracted
  if (file.exists(paste0(f, ".csv"))) {
    delete_if_finished(f)
    next
  }
  
  # On subsequent iterations retrieve data from asynchronous import
  if (!exists('data_next')) {
    data <- split(system(paste0('cat ', f), intern = TRUE), 1:32)  
  } else {
    data <- value(data_next)
  }
  
  # Read next ndjson chunk asynchronously
  if (j < length(uncompressed_files[[i]])) {
    data_next <- future(
      split(system(paste0('cat ', uncompressed_files[[i]][j+1]), intern = TRUE), 1:32)
    )
  }
  
  # Convert ndjson data to data.table fitting database schema
  df <- parallel::mclapply(
    data, 
    function(x) {
      dt <- as.data.table(ndjson::flatten(x))[
        ,file := gsub("\\.*$", "", basename(compressed_files[[i]]))
      ]
      colnames(dt) <- gsub("\\.", "_", colnames(dt))
      
      for (col in setdiff(cols, colnames(dt))) {
        dt[[col]] <- NA
      }
      
      dt[,intersect(cols, colnames(dt)), with = FALSE]
      
    }, 
    mc.cores = 32
  ) %>%
    rbindlist(fill = TRUE)
  
  df[
    ,c("created_utc", "approved_at_utc", "author_created_utc", "banned_at_utc") := list(
      lubridate::as_datetime(created_utc),
      lubridate::as_datetime(approved_at_utc),
      lubridate::as_datetime(author_created_utc),
      lubridate::as_datetime(banned_at_utc)
    )
  ][
    ,created_date := lubridate::as_date(created_utc)
  ]
  
  # Write csv to disk (process #3 will add csv files to database)
  fwrite(df, paste0(f, ".csv"))
  delete_if_finished(f)
  
  rm(df, data)
  gc()
  
}

