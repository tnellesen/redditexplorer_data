library(RSQLite)
library(data.table)
library(magrittr)

db_version <- 'v2.0'

##### Step 1: Get % of submissions marked "over_18" in each subreddit ####
db <- dbConnect(SQLite(), 'storage/RS.db')
nsfw_data <- as.data.table(
  dbGetQuery(
    db, 
    paste0(
      'SELECT subreddit, sum(over_18) as nsfw, count(*) as count ',
      'FROM `all`',
      'GROUP BY subreddit'
    )
  )
)

nsfw_data[
  !is.na(subreddit)
][
  ,
  ratio := nsfw/count
][
  order(-count)
] %>%
  fwrite(paste0('output/nsfw_', db_version, '.csv'))

dbDisconnect(db)


##### Step 2: Get number of comments by subreddit ####

db <- dbConnect(SQLite(), 'storage/RC.db')

subreddit_count_data <- as.data.table(
  dbGetQuery(
    db,
    paste0(
      'SELECT subreddit, count(*) AS count ',
      'FROM `all` ',
      'GROUP BY subreddit'
    )
  )
)

subreddit_count_data[
  !is.na(subreddit)
][
  order(-count)
] %>%
  fwrite(paste0('output/subreddit_comment_counts_', db_version, '.csv'))

dbDisconnect(db)


