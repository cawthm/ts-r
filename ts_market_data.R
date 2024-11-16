library(httr2)
library(data.table)
source("ts_helpers.R")

#' Fetch Historical Stock Data
#'
#' @param symbol character: The stock symbol
#' @param interval character: Time interval ('1min', '5min', 'daily', etc.)
#' @param start_date character: Start date in 'YYYY-MM-DD' format (optional)
#' @param end_date character: End date in 'YYYY-MM-DD' format (optional)
#' @param access_token character: Valid TradeStation access token
#' @return data.table: Historical price data
#' @export
get_stock_history <- function(symbol, interval = "daily", 
                            start_date = NULL, end_date = NULL, 
                            access_token) {
  # API endpoint
  base_url <- "https://api.tradestation.com/v3/marketdata/barcharts/"
  
  # Define interval mappings
  interval_map <- list(
    "1min" = list(interval = 1, unit = "Minute"),
    "2min" = list(interval = 2, unit = "Minute"),
    "3min" = list(interval = 3, unit = "Minute"),
    "5min" = list(interval = 5, unit = "Minute"),
    "10min" = list(interval = 10, unit = "Minute"),
    "15min" = list(interval = 15, unit = "Minute"),
    "30min" = list(interval = 30, unit = "Minute"),
    "hourly" = list(interval = 60, unit = "Minute"),
    "2hour" = list(interval = 120, unit = "Minute"),
    "4hour" = list(interval = 240, unit = "Minute"),
    "daily" = list(interval = 1, unit = "Daily")
  )
  
  # Validate interval
  if (!interval %in% names(interval_map)) {
    stop("Invalid interval. Must be one of: ", paste(names(interval_map), collapse = ", "))
  }
  
  # Initialize request
  req <- request(base_url) %>%
    req_url_path_append(symbol) %>%
    req_headers(Authorization = paste("Bearer", access_token))
  
  # Add interval parameters
  req <- req %>%
    req_url_query(
      interval = interval_map[[interval]]$interval,
      unit = interval_map[[interval]]$unit
    )
  
  # Add date parameters if provided
  if (!is.null(start_date)) {
    req <- req %>%
      req_url_query(
        firstdate = format(as.POSIXct(start_date), "%Y-%m-%dT00:00:00Z", tz = "UTC")
      )
  }
  
  if (!is.null(end_date)) {
    req <- req %>%
      req_url_query(
        lastdate = format(as.POSIXct(end_date), "%Y-%m-%dT23:59:59Z", tz = "UTC")
      )
  }
  
  # Perform request
  response <- req %>% req_perform()
  
  # Parse response
  data <- yyjsonr::read_json_str(rawToChar(response$body))
  
  # Check if we have data
  if (length(data$Bars) == 0) {
    stop("No data returned from API")
  }
  
  # Convert list to data.table directly
  dt <- as.data.table(data$Bars)
  
  # Convert types
  dt[, `:=`(
    timestamp = as.POSIXct(TimeStamp, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    open = as.numeric(Open),
    high = as.numeric(High),
    low = as.numeric(Low),
    close = as.numeric(Close),
    volume = as.numeric(TotalVolume)
  )]
  
  # Select and order columns
  dt <- dt[, .(
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    up_volume = as.numeric(UpVolume),
    down_volume = as.numeric(DownVolume),
    up_ticks = as.numeric(UpTicks),
    down_ticks = as.numeric(DownTicks)
  )]
  
  setkey(dt, timestamp)
  return(dt)
}

#' Fetch Historical Options Data
#'
#' @param symbol character: The option symbol in TradeStation format (e.g., "SPY_240216C500")
#' @param interval character: Time interval ('1min', '5min', 'daily', etc.)
#' @param start_date character: Start date in 'YYYY-MM-DD' format (optional)
#' @param end_date character: End date in 'YYYY-MM-DD' format (optional)
#' @param access_token character: Valid TradeStation access token
#' @return data.table: Historical options data
#' @export
get_option_history <- function(symbol, interval = "daily", 
                             start_date = NULL, end_date = NULL, 
                             access_token) {
  # API endpoint
  base_url <- "https://api.tradestation.com/v3/marketdata/barcharts/"
  
  # Define interval mappings
  interval_map <- list(
    "1min" = list(interval = 1, unit = "Minute"),
    "2min" = list(interval = 2, unit = "Minute"),
    "3min" = list(interval = 3, unit = "Minute"),
    "5min" = list(interval = 5, unit = "Minute"),
    "10min" = list(interval = 10, unit = "Minute"),
    "15min" = list(interval = 15, unit = "Minute"),
    "30min" = list(interval = 30, unit = "Minute"),
    "hourly" = list(interval = 60, unit = "Minute"),
    "2hour" = list(interval = 120, unit = "Minute"),
    "4hour" = list(interval = 240, unit = "Minute"),
    "daily" = list(interval = 1, unit = "Daily")
  )
  
  # Validate interval
  if (!interval %in% names(interval_map)) {
    stop("Invalid interval. Must be one of: ", paste(names(interval_map), collapse = ", "))
  }
  
  # Initialize request
  req <- request(base_url) %>%
    req_url_path_append(symbol) %>%
    req_headers(Authorization = paste("Bearer", access_token))
  
  # Add interval parameters
  req <- req %>%
    req_url_query(
      interval = interval_map[[interval]]$interval,
      unit = interval_map[[interval]]$unit
    )
  
  # Add date parameters if provided
  if (!is.null(start_date)) {
    req <- req %>%
      req_url_query(
        firstdate = format(as.POSIXct(start_date), "%Y-%m-%dT00:00:00Z", tz = "UTC")
      )
  }
  
  if (!is.null(end_date)) {
    req <- req %>%
      req_url_query(
        lastdate = format(as.POSIXct(end_date), "%Y-%m-%dT23:59:59Z", tz = "UTC")
      )
  }
  
  # Perform request
  response <- req %>% req_perform()
  
  # Parse response
  data <- yyjsonr::read_json_str(rawToChar(response$body))
  
  # Check if we have data
  if (length(data$Bars) == 0) {
    stop("No data returned from API")
  }
  
  # Convert list to data.table directly
  dt <- as.data.table(data$Bars)
  
  # Convert types
  dt[, `:=`(
    timestamp = as.POSIXct(TimeStamp, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    open = as.numeric(Open),
    high = as.numeric(High),
    low = as.numeric(Low),
    close = as.numeric(Close),
    volume = as.numeric(TotalVolume),
    open_interest = as.numeric(OpenInterest)  # Added for options
  )]
  
  # Select and order columns
  dt <- dt[, .(
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    open_interest,
    up_volume = UpVolume,
    down_volume = DownVolume,
    up_ticks = UpTicks,
    down_ticks = DownTicks,
    bar_status = BarStatus
  )]
  
  setkey(dt, timestamp)
  return(dt)
}

#' Get Available Option Strikes for a Symbol
#'
#' @param symbol character: The underlying symbol (e.g., "SPY")
#' @param access_token character: Valid TradeStation access token
#' @return data.table: Available option strikes and their details
#' @export
get_option_strikes <- function(symbol, access_token) {
  base_url <- "https://api.tradestation.com/v3/marketdata/options/strikes/"
  
  response <- request(base_url) %>%
    req_url_path_append(symbol) %>%
    req_headers(
      Authorization = paste("Bearer", access_token)
    ) %>%
    req_perform()
  
  # Print raw response for debugging
  raw_content <- rawToChar(response$body)
  print("Raw API Response:")
  print(raw_content)
  
  # Parse response
  data <- yyjsonr::read_json_str(raw_content)
  print("Parsed JSON structure:")
  print(str(data))
  
  # Convert to data.table
  dt <- data.table(Strike = unlist(data$Strikes))
  
  if (nrow(dt) == 0) {
    stop("No option strikes available for symbol: ", symbol)
  }
  
  return(dt)
}

#' Get Available Option Expirations for a Symbol
#'
#' @param symbol character: The underlying symbol (e.g., "SPY")
#' @param access_token character: Valid TradeStation access token
#' @return data.table: Available option expiration dates
#' @export
get_option_expirations <- function(symbol, access_token) {
  base_url <- "https://api.tradestation.com/v3/marketdata/options/expirations/"
  
  response <- request(base_url) %>%
    req_url_path_append(symbol) %>%
    req_headers(
      Authorization = paste("Bearer", access_token)
    ) %>%
    req_perform()
  
  # Parse response
  data <- yyjsonr::read_json_str(rawToChar(response$body))
  
  # Convert to data.table
  dt <- data.table(
    expiration = as.Date(unlist(data$Expirations), format = "%Y-%m-%d")
  )
  
  setkey(dt, expiration)
  return(dt)
}

#' Get Option Chain for a Symbol (Streaming)
#'
#' @param symbol character: The underlying symbol (e.g., "SPY")
#' @param strikes numeric vector: Optional vector of strikes to filter (default: NULL for all)
#' @param expirations date vector: Optional vector of expiration dates to filter (default: NULL for all)
#' @param access_token character: Valid TradeStation access token
#' @param max_records integer: Maximum number of records to collect before stopping (default: 10)
#' @param verbose logical: Whether to print status messages (default: FALSE)
#' @return data.table: Option chain data
#' @export
get_option_chain <- function(symbol, strikes = NULL, expirations = NULL, access_token, max_records = 1000, verbose = FALSE) {
  base_url <- "https://api.tradestation.com/v3/marketdata/stream/options/chains"
  
  # Initialize storage for records with a hash table to prevent duplicates
  parent_env <- environment()
  parent_env$records <- list()
  parent_env$seen_contracts <- new.env(hash = TRUE)
  
  callback <- function(data) {
    tryCatch({
      # Skip heartbeat messages
      if (!is.null(data$Heartbeat)) {
        return(TRUE)
      }
      
      # Skip status messages
      if (!is.null(data$StreamStatus) || !is.null(data$Error)) {
        return(TRUE)
      }
      
      # Skip if no Legs data (defensive programming)
      if (is.null(data$Legs) || length(data$Legs) == 0) {
        return(TRUE)
      }
      
      leg <- data$Legs[1,]
      
      # Create unique key for this contract
      contract_key <- paste(leg$Symbol, data$Side, leg$StrikePrice, leg$Expiration)
      
      # Skip if we've seen this contract before
      if (!is.null(parent_env$seen_contracts[[contract_key]])) {
        return(TRUE)
      }
      
      # Mark this contract as seen
      parent_env$seen_contracts[[contract_key]] <- TRUE
      
      # Validate strike is in requested range
      strike_num <- as.numeric(leg$StrikePrice)
      if (!is.null(strikes) && !(strike_num %in% strikes)) {
        return(TRUE)
      }
      
      # Validate expiration is in requested range
      exp_date <- as.Date(leg$Expiration)
      if (!is.null(expirations) && !(format(exp_date, "%Y-%m-%d") %in% format(as.Date(expirations), "%Y-%m-%d"))) {
        return(TRUE)
      }
      
      record <- list(
        symbol = leg$Symbol,
        strike = strike_num,
        side = data$Side,
        option_type = leg$OptionType,
        expiration = leg$Expiration,
        bid = as.numeric(data$Bid),
        ask = as.numeric(data$Ask),
        last = as.numeric(data$Last),
        volume = as.integer(data$Volume),
        open_interest = as.integer(data$DailyOpenInterest),
        delta = as.numeric(data$Delta),
        theta = as.numeric(data$Theta),
        gamma = as.numeric(data$Gamma),
        vega = as.numeric(data$Vega),
        implied_vol = as.numeric(data$ImpliedVolatility),
        intrinsic = as.numeric(data$IntrinsicValue),
        extrinsic = as.numeric(data$ExtrinsicValue),
        theoretical = as.numeric(data$TheoreticalValue),
        mid = as.numeric(data$Mid),
        bid_size = as.integer(data$BidSize),
        ask_size = as.integer(data$AskSize),
        timestamp = Sys.time()
      )
      
      parent_env$records[[length(parent_env$records) + 1]] <- record
      
      if (verbose) {
        print(paste("Processed record:", length(parent_env$records), 
                   "- Strike:", record$strike, 
                   record$option_type,
                   "Exp:", format(as.Date(leg$Expiration), "%Y-%m-%d")))
      }
      
      if (length(parent_env$records) >= max_records) {
        return(FALSE)
      }
      return(TRUE)
      
    }, error = function(e) {
      if (verbose) {
        print(paste("Error processing record:", e$message))
        if (!is.null(data$Heartbeat)) {
          print("Skipping heartbeat message")
        } else {
          print(str(data))
        }
      }
      return(TRUE)
    })
  }
  
  # Build request
  req <- request(base_url) %>%
    req_url_path_append(symbol)
  
  # Add query parameters
  query_params <- list()
  
  if (!is.null(strikes)) {
    query_params$strikes <- paste(strikes, collapse = ",")
  }
  
  if (!is.null(expirations)) {
    formatted_dates <- sapply(expirations, function(d) {
      if (inherits(d, "Date")) {
        format(d, "%Y-%m-%d")
      } else if (inherits(d, "character")) {
        format(as.Date(d), "%Y-%m-%d")
      } else {
        stop("Expirations must be Date objects or character strings in YYYY-MM-DD format")
      }
    })
    query_params$expirations <- paste(formatted_dates, collapse = ",")
  }
  
  if (length(query_params) > 0) {
    req <- req %>% req_url_query(!!!query_params)
  }
  
  req <- req %>%
    req_headers(Authorization = paste("Bearer", access_token))
  
  # Use stream handler
  req %>%
    req_perform_stream(
      handle_ts_stream(callback, max_records = max_records, verbose = verbose)
    )
  
  # Debug output
  if (verbose) {
    print(paste("Total records collected:", length(parent_env$records)))
    if (length(parent_env$records) > 0) {
      print("First record structure:")
      print(str(parent_env$records[[1]]))
    }
  }
  
  # Convert records to data.table and add derived columns
  if (length(parent_env$records) > 0) {
    tryCatch({
      dt <- rbindlist(parent_env$records, fill = TRUE)
      
      if (verbose) {
        print("Data.table created with dimensions:")
        print(dim(dt))
      }
      
      # Convert expiration to POSIXct
      dt[, expiration := as.POSIXct(expiration, format="%Y-%m-%dT%H:%M:%SZ", tz="UTC")]
      
      # Add useful derived columns
      dt[, `:=`(
        spread = ask - bid,
        spread_pct = (ask - bid) / mid * 100,
        itm = fifelse(
          option_type == "Call",
          strike < last,
          strike > last
        ),
        days_to_expiry = as.numeric(difftime(expiration, Sys.time(), units = "days"))
      )]
      
      # Set key for faster lookups
      setkey(dt, strike, option_type, expiration)
      
      return(dt)
    }, error = function(e) {
      if (verbose) {
        print(paste("Error creating data.table:", e$message))
      }
      return(data.table())
    })
  } else {
    if (verbose) {
      print("No records collected!")
    }
    return(data.table())
  }
}