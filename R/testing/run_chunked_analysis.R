

# Notes -------------------------------------------------------------------

# (a) function should be able to make decisions about how to run loops, depending on how many loops need to be run,
# the estimated data size for each loop, memory requirements, and processing requirements. 

# (b) function should be flexible according to how outputs should be returned

# (c) function should incorporate several options for running parallel tasks 

# (d) a helper function should exist to make it easy to create partitions upon which analyses will be run 


# Libraries and options ---------------------------------------------------

library(plyr)
library(dplyr)
library(data.table)

# Test data, etc. ---------------------------------------------------------

obs = 1e7

big_data <- 
  data_frame(
    ID = rep(1:1e4, each = 100, length.out = obs), 
    group_one = rep(letters[1:5], length.out = obs), 
    group_two = rep(state.abb[sample(1:length(state.abb))], length.out = obs), 
    x = rnorm(obs), 
    y = 0.5 * x + rnorm(obs, 0, 0.25), 
    z = 0.40 * y + 0.3 * x + rnorm(obs, 0, .3)
  ) 


# Helper parameters -------------------------------------------------------


# Function to add chunk_var -----------------------------------------------

vars_to_loop_over = c("group_one", "group_two")

funtion_to_apply = function(df){
  inner_model = lm(z ~ y + x, data = df)
  return(inner_model)  
}


df = big_data %>% data.table(key = vars_to_loop_over)


unique_loop_var_comb <- unique(df[, vars_to_loop_over, with = F])


system.time({
  models = dlply(big_data, vars_to_loop_over, funtion_to_apply)
})


alply(unique_loop_var_comb, )