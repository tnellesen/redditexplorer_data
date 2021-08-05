
library(purrr)
library(dplyr)
library(data.table)
library(parallel)
library(RSQLite)
library(reticulate)

args <- commandArgs(trailingOnly = TRUE)

use_condaenv("graphembedding", conda = "~/anaconda3/bin/conda", required = TRUE)

gap <- import('graphvite.application')

input_p <- as.numeric(args[1])
input_q <- as.numeric(args[2])
random_walk_length <- as.numeric(args[3])
input_dim <- 512L
version <- "2.0"
total_epochs <- as.numeric(args[4])

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
if (!is.na(input_num_epochs))
  if (input_num_epochs == total_epochs)
    stop()


app <- gap$GraphApplication(dim = input_dim)
app$gpus <- list(0L, 1L)
app$cpu_per_gpu <- as.integer(62L/length(app$gpus))

input_data_file <- 'storage/all_data_small.tsv'

system.time(
  app$load(
    file_name = paste0(input_data_file),
    as_undirected = FALSE
  )
)

app$build(episode_size = 20000L, batch_size = 20000L)

epochs_per_checkpoint <- 20
start_epoch <- 1



for (i in as.integer(epochs_per_checkpoint*start_epoch:(total_epochs/epochs_per_checkpoint))) {
  output_file <- paste0(
    'embeddings/model_',
    version, "_",
    i, '_epochs_',
    if (input_p == 1) "" else paste0('p_', input_p, '_'),
    if (input_q == 1) "" else paste0('q_', input_q, '_'),
    if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'),
    'd_', input_dim
  )

  if (file.exists(output_file))
    next

  app$train(
    augmentation_step = 1L, 
    model = 'node2vec',
    q = input_q,
    p = input_p,
    random_walk_length = as.integer(random_walk_length),
    num_epoch = as.integer(epochs_per_checkpoint),
    log_frequency = 50000L,
    resume = (i != epochs_per_checkpoint)
  )
  
  app$save_model(
    output_file, 
    save_hyperparameter = TRUE
  )
  
  if (!(i/epochs_per_checkpoint) %% 5 == 1) {
    file.remove(
      paste0(
        'embeddings/model_', 
        version, "_", 
        i-epochs_per_checkpoint, '_epochs_', 
        if (input_p == 1) "" else paste0('p_', input_p, '_'), 
        if (input_q == 1) "" else paste0('q_', input_q, '_'), 
        if (random_walk_length == 40) "" else paste0('rwl_', random_walk_length, '_'), 
        'd_', input_dim
      )
    )
  }
  
  if (file.exists('/tmp/stopfile')) {
    start_epoch <- i/as.integer(epochs_per_checkpoint) + 1L
    break
  }
}
