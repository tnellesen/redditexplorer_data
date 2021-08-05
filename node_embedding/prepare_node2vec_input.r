library(purrr)
library(dplyr)
library(data.table)
library(parallel)
library(RSQLite)
library(multidplyr)


##### Step 1 ####

db <- dbConnect(SQLite(), '/mnt/zfs/Reddit/RC.db')
data <- dbGetQuery(db, paste0("SELECT subreddit, author, COUNT(*) AS n FROM  `all` GROUP BY subreddit, author"))
dbDisconnect(db)


data <- as.data.table(data)[
  author != "[deleted]"
][
  ,
  .(n = .N), 
  by = c("author", "subreddit")
]

setkey(data, n)

subreddit_count <- data[, .(n = .N), by = "subreddit"][order(-n)]
data <- data[, .(subreddit, n, n_author = sum(n)), by = "author"]

fwrite(data, "subreddit_author_data_backup.csv")
fwrite(subreddit_count, "subreddit_count_data_backup.csv")

##### Step 2 ####
data <- fread("/mnt/zfs/Reddit/subreddit_author_data_backup.csv")
subreddit_count <- fread("/mnt/zfs/Reddit/subreddit_count_data_backup.csv")

data <- setkey(data, subreddit)

data2 <- data

datalist <- list()
increment <- 1e6
for (i in unique(floor(1:nrow(data)/increment))+1) {
  print(paste0("i = ", i))
  subreddits <- data[((i-1)*increment+1):min(i*increment, nrow(data)),.(subreddit)]
  datalist[[length(datalist)+1]] <- data[subreddit %in% unique(unlist(subreddits)),][, .(author, n, n_author, n_subreddit = sum(n)), by = "subreddit"]
}
data <- rbindlist(datalist)

rm(increment, datalist, i)

fwrite(data, "/mnt/zfs/Reddit/subreddit_author_data_all_counts.csv")



##### Step 3 ####
data <- setkey(data, author)

process_chunk <- function(chunk, stacked = stacked) {
  chunk <- chunk[stacked, on = "author", allow.cartesian = TRUE, nomatch = NULL]
  chunk[, .(prob = sum((n / n_subreddit) * (i.n / i.n_author))), .(subreddit, i.subreddit)]
}

max_id <- 8
subreddits <- unique(data$subreddit) %>%
  split(1:250)
datalist <- list()
i <- 0
i_previous <- c()

for (sr in subreddits[setdiff(1:length(subreddits), i_previous)]) {
  print(
    system.time(
      {
        gc()
        i <- i + 1
        res <- process_chunk(data[subreddit %in% sr,], data)
        print(paste0("i = ", i, " ;subreddit = ", sr, "; rows = ", nrow(res))[1])
        fwrite(res, paste0("/mnt/zfs/Reddit/data_cache_", as.integer(i), ".csv"))
        rm(res)
      }
    )
  )
  i_previous[length(i_previous)+1] <- i
  
  if (file.exists("/tmp/stopfile")) break
  if (file.exists("/tmp/notify")) beepr::beep(1)
}



##### Step 4 ####
files <- list.files("/mnt/zfs/Reddit", full.names = TRUE, pattern = "data_cache_\\d+\\.csv$")

counter <- 1L
for (i in 0:9*25+1) {
  print(paste0("i = ", i))
  if ("prob_data" %in% ls()) {
    rm(prob_data)
    gc()
  }
  prob_data <- map(files[i+0:24], ~fread(., showProgress = FALSE)) %>%
    rbindlist
  fwrite(prob_data, paste0("/mnt/zfs/Reddit/prob_data_", counter, ".csv"))
  counter <- counter + 1L
}

beepr::beep(1)

##### Step 5 ####
output <- rbindlist(datalist)
output2 <- output
output <- setkey(output, subreddit, i.subreddit)

list_length <- 16L
n_cores <- 12L
for (i in 5L:10L) {
  print(paste0("Reading prob_data_", i, ".csv"))
  
  output <- fread(paste0("/mnt/zfs/Reddit/prob_data_", i, ".csv"))[
    prob >= 2.5e-08
  ][
    order(-prob)
  ]
  
  output_list <- split(output, 1:list_length)
  counter <- 0L
  gc()
  for (dataset in output_list) {
    print(paste0("Iteration ", counter, ":"))
    res <- dataset %>%
      split(1:n_cores) %>% 
      (parallel::mclapply)(
        function(df) {
          apply(
            df[
              , 
              c("subreddit", "i.subreddit", "prob") := list(
                substr(paste0(subreddit, "                     "), 1, 22),
                substr(paste0(i.subreddit, "                     "), 1, 22),
                format(prob, digits = 21, scientific = FALSE)
              )
            ], 
            1, 
            function(x) {paste0(x, collapse = " ")}
          )}, 
        mc.cores = n_cores
      ) %>%
      unlist(use.names = FALSE)
    
    write.table(
      res, 
      col.names = FALSE, 
      row.names = FALSE, 
      quote = FALSE, 
      file = paste0("/mnt/zfs/Reddit/formatted_data_", list_length*(i-1L)+counter+1, ".tsv")
    )
    counter <- counter + 1L
    rm(res)
    gc()
  }
  cat("\n")
  rm(output, output_list)
  gc()
}



##### Step 6 ####
for (file in list.files("/mnt/zfs/Reddit", full.names = TRUE, pattern = "formatted_data_\\d+.tsv")) 
  system(print(paste0("cat '", file, "' >> /mnt/zfs/Reddit/all_data.tsv")))

for (file in list.files("/mnt/zfs/Reddit", full.names = TRUE, pattern = "formatted_data_\\d+.tsv")) 
  system(print(paste0("cat '", file, "' | head -n 25000000 >> /mnt/zfs/Reddit/all_data_small.tsv")))




