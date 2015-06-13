
library(plyr)
library(dplyr)
library(data.table)
library(foreach)
library(parallel)
library(doSNOW)
library(microbenchmark)

# cl <- makeCluster(3, type = "SOCK")
# registerDoSNOW(cl)
# result <- foreach(counter = 1:100) %dopar% function(counter)
# stopCluster(cl)


# Test data ---------------------------------------------------------------
size = 3e7
iterations = 20

test_data <-
  data.frame(x = rnorm(size)) %>%
  mutate(
    y = x^3 + runif(length(x), 0, 1), 
    group_one = rep(letters[1:3], length.out = length(x)), 
    group_two = rep(1:1e7, length.out = length(x))
    ) %>%
  data.table()


# Add iterators to data ---------------------------------------------------

unique_iterators <-
  data.frame(unique_group_var = base::unique(test_data$group_two)) %>%
  mutate(
    iterator_var = rep(1:20, length.out = length(unique_group_var))
    ) %>%
  data.table(key = "unique_group_var")

test_data_iterators <-
  test_data[unique_iterators]

setkey(test_data_iterators, iterator_var)

# cl <- makeCluster(3, type = "SOCK")
# registerDoSNOW(cl)
# 
# system.time({  
#   results <- foreach(it = 1:20, .combine = rbindlist) %dopar% mean_function(it, test_data_iterators)
#   })
# 
# stopCluster(cl)

system.time({
  summary_function(data = test_data_iterators)
})