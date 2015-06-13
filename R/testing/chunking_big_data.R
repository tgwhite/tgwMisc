

# Libraries and options ---------------------------------------------------

library(dplyr)
library(data.table)


# Helper parameters -------------------------------------------------------

obs = 1e7 # 10 million obs
out_dir = "C:/Users/taylor/Dropbox/R_programming/stackoverflow/data"

# Create example file and write to disk -----------------------------------

example_df <- 
  data_frame(
    x = 1:obs, 
    y = 0.5 * x + rnorm(length(x), 0, .25), 
    group = rep(letters[1:3], length.out = length(x))
  )

# export
system.time({
  write.csv(example_df, paste0(out_dir, "/large_df_example.csv"), row.names = F)
})

# clean memory
rm(list = ls()[!(ls() %in% "out_dir")])
gc()


# Use for loop and read.table for chunked import --------------------------

# let's pretend we don't know how many obs there are in the file because if the file is too large to 
# read into memory, we won't know

chunk_size = 5e5 # 500k
max_estimated_size = 1e8 # 100 million
max_chunks = ceiling(max_estimated_size/chunk_size) # the loop will simply error when there are insufficient obs left to read in 

system.time({
  for(i in 1:max_chunks){
    
    df <- read.csv(paste0(out_dir, "/large_df_example.csv"), header = TRUE, nrow = chunk_size, skip = chunk_size * (i-1))  
    write.table(df, paste0(out_dir, "/df", i, ".txt"))
    
    if (nrow(df) < chunk_size) {
      total_obs = ((i-1) * chunk_size) + nrow(df) 
      cat("number of chunks equals", i, "and total obs equals", total_obs, "\n")
      break
    }
  }
    
})


# Now let's replace read.csv() with fread() -------------------------------
rm(list = ls()[!(ls() %in% c("max_chunks", "chunk_size", "out_dir"))])
gc()

system.time({
  for(i in 1:max_chunks){
    
    df <- fread(paste0(out_dir, "/large_df_example.csv"), header = TRUE, nrow = chunk_size, skip = chunk_size * (i-1))  
    write.table(df, paste0(out_dir, "/df", i, ".txt"))
    
    if (nrow(df) < chunk_size) {
      total_obs = ((i-1) * chunk_size) + nrow(df) 
      cat("number of chunks equals", i, "and total obs equals", total_obs, "\n")
      break
    }
  }
  
})

### That's a ~4x speedup!!! ###

# Replace for loop with lapply() ------------------------------------------

# clean memory
rm(list = ls()[!(ls() %in% c("max_chunks", "chunk_size", "out_dir"))])
gc()

# test the file by skipping the max guess and reading those files in. 
system.time({
  lapply(1:max_chunks, function(i){
    
    df <- fread(paste0(out_dir, "/large_df_example.csv"), header = TRUE, nrow = chunk_size, skip = chunk_size * (i-1))  
    write.table(df, paste0(out_dir, "/df", i, ".txt"))
    
    if (nrow(df) < chunk_size) {
      total_obs = ((i-1) * chunk_size) + nrow(df) 
      cat("number of chunks equals", i, "and total obs equals", total_obs, "\n")
      
      stop("zero additional obs to read in, stopping lapply loop\n")
      
    } else {
      rm(df)
      gc()
    }      
  })
  
})
?s