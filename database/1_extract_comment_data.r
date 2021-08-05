library(purrr)
library(data.table)

compressed_files <- list.files(
  '/data/drv2/Reddit/Raw',
  pattern = 'RC_',
  full.names = TRUE
) %>%
  keep(~grepl("\\.(bz2|xz|zst)$", .))


decompress_file <- function(f) {
  date <- substr(basename(f), 4, 10)
  
  uncompressed <- c(
    list.files(
      file.path("/mnt/zfs/Reddit/Uncompressed", substr(basename(f), 4, 10)),
      full.names = TRUE
    )
  )
  
  if (length(uncompressed) > 0)
    return(uncompressed)
  if (dir.exists(file.path("/mnt/zfs/Reddit/Uncompressed", substr(basename(f), 4, 10))))
    return(uncompressed)
  
  intermediate_file <- file.path('/data/drv3/Reddit/Uncompressed', paste0("RC_", date))
  uncompressed_dir <- file.path('/mnt/zfs/Reddit/Uncompressed', date)
  
  ext <- gsub("^.*\\.", "", f)
  if (!file.exists(intermediate_file)) {
    if (ext == "bz2") {
      system(paste0("pbzip2 -p16 -dc '", f, "' > '", intermediate_file, "'"))
    } else if (ext == "zst") {
      system(paste0("zstd --long=31 -dc '", f, "' > '", intermediate_file, "'"))
    } else if (ext == "xz") {
      system(paste0("xz -T16 -dc '", f, "' > '", intermediate_file, "'"))
    } else stop(paste0("Extension '", ext, "' not supported."))
  }
  
  if (!dir.exists(uncompressed_dir))
    dir.create(uncompressed_dir)
  
  system(paste0("split -C 2G '", intermediate_file, "' '", file.path(uncompressed_dir, paste0(date, "__")), "'"))
  if (abs(1-sum(file.size(grep('csv', list.files(uncompressed_dir, full.names = TRUE), value = TRUE, invert = TRUE)))/file.size(intermediate_file)) < 0.01) {
    file.remove(intermediate_file)
    file.remove(grep('__..2$', grep('csv', list.files(uncompressed_dir, full.names = TRUE), value = TRUE, invert = TRUE), value = TRUE))
  }
  
  return(
    list.files(
      file.path("/mnt/zfs/Reddit/Uncompressed", substr(basename(f), 4, 10)),
      full.names = TRUE
    )
  )
}

shuffled_compressed_files <- sample(compressed_files, length(compressed_files))

for (f in shuffled_compressed_files) {
  print(f)
  res <- decompress_file(f)
}
