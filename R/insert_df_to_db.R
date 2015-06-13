
insert_df_to_db <- function(df, conn, name, size = 1e3, insert_to_existing_table = F){
  
  require(DBI)
  require(tau)
  require(plyr)
  require(dplyr)
  require(stringr)
  
  if (sum(str_detect(names(df), fixed("."))) > 0) {
    cat("df contains column names with a '.', which is problematic for databases -- removing '.' from column names\n")
    setnames(df, names(df), str_replace_all(names(df), pattern = fixed("."), replacement = ""))  
  }
    
  
  ## Helper function ##
  
  get_obj_chunks <- function(x, size){
    
    if (is.vector(x)) {
      n = length(x)  
    } else if (is.matrix(x) | is.data.frame(x)) {
      n = nrow(x)
    } else {
      stop("please supply a vector, matrix, or data.frame")
    }
    
    s = n %/% size
    r = n %% size
    
    if (r == 0) {
      splits = s
    } else {
      splits = s + 1
    }
    
    ends = size * (1:splits)
    ends[length(ends)] = n
    starts = lag(ends) + 1
    starts[1] = 1  
    
    return(data.frame(start = starts, end = ends))
  }  
  
  df = as.data.frame(df)
  
  if (!(dbExistsTable(conn, name))) {
    
    ## convert dates to characters ##     
    date_cols = sapply(df, class)[sapply(df, class) %in% "Date"] %>% names()    
    
    if (length(date_cols) > 0) {
      for (it in 1:length(date_cols)) {
        df[[date_cols[it]]] = as.character(df[[date_cols[it]]])
      }  
    }
    
    cat("table does not exist, creating new table and inserting df\n")    
    dbWriteTable(conn, name, df)
    
    return(NULL)
    
  } else if (insert_to_existing_table) {                            
    
    ## convert NA or NULL characters to NULL and all values to character ##     
    
    type_check = dbSendQuery(db_con, paste("SELECT * FROM", name, "LIMIT 1"))
    type_check_result = dbFetch(type_check)
    check_result = dbGetInfo(type_check)$fields      
    
    quoted_cols <- lapply(check_result$name, function(col_name){                                
      
      stopifnot(col_name %in% names(df))
      
      table_col_class = subset(check_result, name == col_name)$Sclass      
      col = df[[col_name]]
      col_class = class(col)
      
      if (col_class %in% c("Date", "POSIXct", "POSIXt", "character")) {                
        char = T        
      } else if (col_class == "factor") {        
        char = (table_col_class == "character")                      
      } else if (col_class %in% c("numeric", "integer")) {
        char = F        
      } else {        
        stop(paste("column class:", col_class, ",is not currently supported by this function"))
      }
      
      out_col = ifelse(is.na(col) | is.null(col), "NULL", as.character(col))
      
      ## if actually a character column, quote the column appropriately ## 
      if (char) {
        out_col = as.character(dbQuoteString(conn, out_col))
      }  
      
      return(out_col)
    })  
    
    quoted_cols_mat <-  
      matrix(
        unlist(quoted_cols), 
        ncol = ncol(df),   
        nrow = nrow(df), 
        byrow = F
      )
        
    ## concatenate each row ##
    rows_prepped_for_output = apply(quoted_cols_mat, 1, function(row){
      
      sql_insert_row = paste0("(", paste(row, collapse = ", "), ")")      
      tau::translate(sql_insert_row) %>% 
        return()
    })        
    
    ## insert in chunks ## 
    
    start_ends = get_obj_chunks(rows_prepped_for_output, size = size)
    
    ## prep values for insertion ## 
    query_string_start = paste("INSERT INTO", name, paste0("(", paste(names(df), collapse = ", "), ")"), "VALUES")    
    
    print(query_string_start)
    
    ## run insertion query in chunks ##
    
    lapply(1:nrow(start_ends), function(it){
      
      cat("inserting chunk", it, "...\n")      
      
      chunk = rows_prepped_for_output[(start_ends$start[it]:start_ends$end[it])]      
      
      query_string_values = paste(chunk, collapse = ", ")          
      
      dbSendQuery(conn, paste(query_string_start, query_string_values))        
      
      cat("done\n")
      
    })
    
    return(NULL)
    
  } else {
    cat("table already exists and insert_to_existing_table = F so not inserting data\n")
    return(NULL)
  }  
}

