library(httr2)
library(data.table)

#' Handle TradeStation Streaming Data
#'
#' @param stream_callback function: Callback function to process each valid JSON record
#' @param max_records integer: Maximum number of records to process (default: Inf)
#' @param verbose logical: Whether to print status messages (default: FALSE)
#' @return function: A stream handler function compatible with req_perform_stream
#' @export
handle_ts_stream <- function(stream_callback, max_records = Inf, verbose = FALSE) {
  # Create environment to store state
  state <- new.env()
  state$buffer <- ""
  state$record_count <- 0
  
  function(resp) {
    # Append new chunk to buffer
    chunk <- rawToChar(resp)
    state$buffer <- paste0(state$buffer, chunk)
    
    # Process complete JSON objects
    while (TRUE) {
      # Find next newline
      json_end <- regexpr("\r\n", state$buffer)[1]
      if (json_end == -1) break  # No complete JSON object yet
      
      # Extract JSON string
      json_str <- substr(state$buffer, 1, json_end - 1)
      state$buffer <- substr(state$buffer, json_end + 2, nchar(state$buffer))
      
      tryCatch({
        data <- yyjsonr::read_json_str(json_str)
        
        # Handle stream status messages
        if (!is.null(data$StreamStatus)) {
          if (verbose) print(paste("Stream status:", data$StreamStatus))
          if (data$StreamStatus == "GoAway") {
            return(FALSE)  # Stop streaming
          }
          next
        }
        
        # Handle error messages
        if (!is.null(data$Error)) {
          if (verbose) print(paste("Stream error:", data$Error))
          return(FALSE)  # Stop streaming
        }
        
        # Process valid data through callback
        stream_callback(data)
        state$record_count <- state$record_count + 1
        
        if (verbose) print(paste("Processed record:", state$record_count))
        
        # Check record limit
        if (state$record_count >= max_records) {
          return(FALSE)  # Stop streaming
        }
        
      }, error = function(e) {
        if (verbose) {
          print(paste("Error parsing JSON:", e$message))
          print(paste("JSON string:", json_str))
        }
      })
    }
    return(TRUE)  # Continue streaming
  }
}