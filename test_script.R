source("ts_auth.R")
source("ts_helpers.R")
source("ts_market_data.R")

# Get fresh access token
tokens <- get_access_token()
tokens

# Test stock history
# Test 1: Basic daily data (should work as before)
spy_daily <- get_stock_history(
  symbol = "SPY",
  access_token = tokens$access_token
)

print("Daily data:")
print(spy_daily)

# Test 2: 5-minute data with date range
spy_5min <- get_stock_history(
  symbol = "SPY",
  interval = "5min",
  start_date = "2024-01-30",
  end_date = "2024-01-31",
  access_token = tokens$access_token
)
print("\n5-minute data:")
print(spy_5min)

# Test 3: Hourly data
spy_hourly <- get_stock_history(
  symbol = "SPY",
  interval = "hourly",
  start_date = "2024-01-15",
  end_date = "2024-01-31",
  access_token = tokens$access_token
)
print("\nHourly data:")
print(spy_hourly)

# Test option history (example option symbol)
spy_option_data <- get_option_history(
  symbol = "SPY_20240119C500",  # Adjust this to a valid option symbol
  interval = "daily",
  start_date = "2024-01-01",
  end_date = "2024-01-31",
  access_token = tokens$access_token
)

print(spy_option_data)

# Get fresh token if needed
tokens <- get_access_token()

# Get available strikes for SPY
spy_strikes <- get_option_strikes(
  symbol = "SPY",
  access_token = tokens$access_token
)

print(spy_strikes)
########################
# testing options
########################
# Get fresh token if needed

tokens <- get_access_token()
tokens

source("ts_market_data.R")

# Get available expirations
spy_expirations <- get_option_expirations(
  symbol = "SPY",
  access_token = tokens$access_token
)
print("Available Expirations:")
print(spy_expirations)

# Get option chain for near-the-money strikes
print("\nStreaming Option Chain:")
spy_chain <- get_option_chain(
  symbol = "SPY",
  strikes = c(485:495),
  expirations = c("2024-11-19", "2024-11-20"),  # Can use dates or character strings
  access_token = tokens$access_token,
  max_records = 200,
  verbose = TRUE
)

# Check the structure
str(spy_chain)

# print it out
print(spy_chain)

# Look at some summary stats
spy_chain[, .(
  avg_spread = mean(spread),
  avg_spread_pct = mean(spread_pct),
  itm_count = sum(itm),
  total_volume = sum(volume)
), by = .(option_type)]
