### Note: this code has not yet been updated based on revised database, using DuckDB instead of 
### SQLite and including data for 2021

p <- 4
q <- 4
rwl <- 5
counts <- c(10000, 25000, 50000, 100000, 250000, 500000)

node2vec_epochs <- 500
largevis_epochs <- 2000

system(
  paste(
    'Rscript --vanilla scripts/run_node2vec.r', p, q, rwl, node2vec_epochs, '&> logs/log_node2vec.txt'
  )
)

system(
  paste(
    'Rscript --vanilla scripts/run_largevis.r', p, q, rwl, largevis_epochs, '&> logs/log_largevis.txt'
  )
)

for (count in counts) {
  system(
    paste0(
      'scripts/precompute_script.sh "output/largevis_2.0_', 
      largevis_epochs, '_epochs_', node2vec_epochs, '_input_epochs_', 
      if (p != 1) paste0('p_', p, '_'), 
      if (q != 1) paste0('q_', q, '_'), 
      if (rwl != 40) paste0('rwl_', rwl, '_'), 
      'd_512-3.csv" ',
      format(count, digits = 7, scientific = FALSE), ' ', p, ' ', q, ' ', rwl, ' ',
      (
        outfile <- paste0(
          'cluster_data/cluster_data_v2.0_', 
          if (p != 1) paste0('p_', p, '_'), 
          if (q != 1) paste0('q_', q, '_'), 
          if (rwl != 40) paste0('rwl_', rwl, '_'),
          format(count, digits = 7, scientific = FALSE), 
          '.json'
        )
      )
    )
  )
  
  source('scripts/server_functions.r')
  upload_file(outfile)

}





