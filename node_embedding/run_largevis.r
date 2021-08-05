
library(purrr)
library(dplyr)
library(data.table)
library(parallel)
library(RSQLite)
library(reticulate)

args <- commandArgs(trailingOnly = TRUE)

use_condaenv("graphembedding", conda = "~/anaconda3/bin/conda", required = TRUE)

py <- import_builtins(convert = FALSE)
gap <- import('graphvite.application')

get_vertices <- function(location, subreddits = NULL) {
  pkl <- import('pickle')
  f <- py$open(location, 'rb')
  model <- pkl$load(f)
  
  if (!inherits(tryCatch(model$solver$vertex_embeddings, error = function(e) e), "error")) {
    vertices <- as.data.frame(model$solver$vertex_embeddings)
  } else {
    vertices <- as.data.frame(model$solver$coordinates)
  }
  
  dim <- ncol(vertices)
  
  if (is.null(subreddits))
    vertices$subreddit <- model$graph$id2name
  else vertices$subreddit <- subreddits
  
  colnames(vertices) <- c(paste0("V", 1L:dim), "subreddit")
  vertices <- vertices[, c(dim+1, 1:dim)]
  rownames(vertices) <- NULL
  as.data.table(vertices)
}


version <- "2.0"
input_p <- as.numeric(args[1])
input_q <- as.numeric(args[2])
random_walk_length <- as.numeric(args[3])
input_dim <- 512L
output_dim <- 3L

input_num_epochs <- list.files(".", pattern = "model_", recursive = TRUE) %>%
  gsub("model_", "", .) %>%
  keep(~grepl(paste0("p_", input_p), .)|((!grepl("p_", .))&(input_p == 1))) %>%
  keep(~grepl(paste0("q_", input_q), .)|((!grepl("q_", .))&(input_q == 1))) %>%
  keep(~grepl(paste0("rwl_", random_walk_length), .)|((!grepl("rwl_", .))&(random_walk_length == 40))) %>%
  keep(~grepl(paste0("d_", input_dim), .)) %>%
  basename %>%
  gsub(paste0(version, "_"), "", ., fixed = TRUE) %>%
  stringr::str_extract("^\\d+") %>% 
  as.integer %>% 
  max(na.rm = TRUE)



app <- gap$VisualizationApplication(dim = output_dim)
app$gpus <- list(0L, 1L)
app$cpu_per_gpu <- as.integer(62L/length(app$gpus))

vertices <- get_vertices(
  list.files(
    ".", 
    recursive = TRUE, 
    full.names = TRUE, 
    pattern = paste0(
      
      "model_", 
      version, "_", 
      input_num_epochs, "_epochs_", 
      if (input_p != 1) paste0("p_", input_p, "_") else "", 
      if (input_q != 1) paste0("q_", input_q, "_") else "", 
      if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
      "d_", input_dim
    )
  )
)

fwrite(
  vertices,
  paste0(
    "output/model_", 
    version, "_", 
    input_num_epochs, "_epochs_", 
    if (input_p != 1) paste0("p_", input_p, "_") else "", 
    if (input_q != 1) paste0("q_", input_q, "_") else "", 
    if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
    "d_", input_dim, ".csv"
  )
)


app$load(
  vectors = as.matrix(vertices[,setdiff(colnames(vertices), "subreddit"), with = FALSE])
)

app$build()

epochs <- as.numeric(args[4])
num_checkpoints <- 20
epochs_per_checkpoint <- as.integer(epochs/num_checkpoints)
start_epoch <- 1

most_recent_index <- list.files(
  "embeddings", 
  recursive = TRUE, 
  full.names = TRUE
) %>%
  keep(~grepl(paste0("p_", input_p), .)|((!grepl("p_", .))&(input_p == 1))) %>%
  keep(~grepl(paste0("q_", input_q), .)|((!grepl("q_", .))&(input_q == 1))) %>%
  keep(~grepl(paste0("rwl_", input_p), .)|((!grepl("rwl_", .))&(random_walk_length == 40))) %>%
  keep(~grepl(paste0("d_", input_dim), .)) %>%
  gsub(paste0("embeddings/largevis_", version, "_"), "", ., fixed = TRUE)  %>%
  stringr::str_extract("^\\d+") %>% 
  as.numeric %>% 
  max

if (most_recent_index > 0) {
  model_file <- paste0(
    'embeddings/largevis_',
    version, '_',
    most_recent_index, '_epochs_',
    input_num_epochs, '_input_epochs_',
    if (input_p != 1) {paste0('p_', input_p, '_')},
    if (input_q != 1) {paste0('q_', input_q, '_')},
    if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'),
    'd_', input_dim, '-', output_dim
  )
  
  app$load_model(model_file)
}

for (i in epochs_per_checkpoint*start_epoch:num_checkpoints) {
  if (i <= most_recent_index)  
    next

  app$train(
    model = 'LargeVis',
    num_epoch = epochs_per_checkpoint,
    log_frequency = 10000L,
    resume = (i != epochs_per_checkpoint)
  )
  
  app$save_model(
    paste0(
      'embeddings/largevis_', 
      version, '_', 
      i, '_epochs_', 
      input_num_epochs, '_input_epochs_',
      if (input_p != 1) {paste0('p_', input_p, '_')}, 
      if (input_q != 1) {paste0('q_', input_q, '_')}, 
      if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
      'd_', input_dim, '-', output_dim
    ), 
    save_hyperparameter = TRUE
  )
  
  if (!(i/epochs_per_checkpoint) %% 5 == 1) {
    file.remove(
      paste0(
        'embeddings/largevis_', 
        version, '_', 
        i-epochs_per_checkpoint, '_epochs_', 
        input_num_epochs, '_input_epochs_',
        if (input_p != 1) {paste0('p_', input_p, '_')}, 
        if (input_q != 1) {paste0('q_', input_q, '_')}, 
        if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
        'd_', input_dim, '-', output_dim
      )
    )
  }
  
  beepr::beep(4)
  if (file.exists('stop')) break
}



new_vertices <- get_vertices(
  paste0(
    'embeddings/largevis_', 
    version, '_', 
    num_checkpoints*epochs_per_checkpoint, '_epochs_', 
    input_num_epochs, '_input_epochs_',
    if (input_p != 1) {paste0('p_', input_p, '_')}, 
    if (input_q != 1) {paste0('q_', input_q, '_')}, 
    if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
    'd_', input_dim, '-', output_dim
  ), 
  vertices$subreddit
)

if (length(list.files("output", pattern = paste0("subreddit_comment_counts_v", version))) < 1) {
  db <- dbConnect(SQLite(), "/mnt/zfs/Reddit/RC.db")
  data <- as.data.table(dbGetQuery(db, "SELECT subreddit, count(*) as count from `all` GROUP BY subreddit"))
  dbDisconnect(db)
  
  fwrite(
    data[order(-count)], 
    file <- paste0("output/subreddit_comment_counts_v", version,".csv")
  )
} else {
  file <- list.files("output", pattern = paste0("subreddit_comment_counts_v", version), full.names = TRUE)[1]
}

sorted_new_vertices <- new_vertices[
  fread(file), 
  on = "subreddit"
][
  !is.na(V1)
][
  order(-count)
]

fwrite(
  sorted_new_vertices[,.(subreddit, V1, V2, V3)],
  paste0(
    'output/largevis_', 
    version, '_',
    num_checkpoints*epochs_per_checkpoint, '_epochs_', 
    input_num_epochs, '_input_epochs_',
    if (input_p != 1) {paste0('p_', input_p, '_')},
    if (input_q != 1) {paste0('q_', input_q, '_')},
    if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
    'd_', input_dim, '-', output_dim,
    ".csv"
  )
)





