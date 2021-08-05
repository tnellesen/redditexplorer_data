
library(purrr)
library(data.table)
library(duckdb)

compressed_files <- list.files(
  '/data/drv2/Reddit/Raw',
  pattern = 'RC_',
  full.names = TRUE
) %>%
  keep(~grepl("\\.(bz2|xz|zst)$", .))

repeat {
  uncompressed_files <- map(
    rev(compressed_files), ~{
      list.files(
        paste0("/mnt/zfs/Reddit/Uncompressed/", substr(basename(.), 4, 10)), 
        full.names = TRUE, 
        pattern = "__..\\.csv$"
      )
    }
  ) %>%
    unlist %>%
    sample
  
  Sys.sleep(5)
  
  db <- dbConnect(duckdb::duckdb(), '/mnt/zfs/Reddit/RC_new.db')
  dbExecute(db, "PRAGMA memory_limit='50GB'")
  for (f in uncompressed_files) {
    print(f)  
    a <- print(duckdb::duckdb_read_csv(db, 'comments', f))
    file.remove(f)  
  }
  dbDisconnect(db, shutdown = TRUE)

  print("Restarting in 15 seconds.")
  Sys.sleep(15)
}

