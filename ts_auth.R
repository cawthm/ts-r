library(httr2)
library(stringr)
library(yyjsonr)

#' Get Fresh Access Token using Refresh Token
#'
#' @param client_creds list: List containing client_id and client_secret
#' @return list: A list containing the new access token and other details
#' @export
get_access_token <- function() {
  # Load credentials and refresh token
  client_creds <- readRDS(".secrets/.client_creds.rds")
  refresh_token <- readRDS(".secrets/.refresh_token.rds")
  
  token_url <- "https://signin.tradestation.com/oauth/token"
  
  response <- request(token_url) %>%
    req_method("POST") %>%
    req_headers("Content-Type" = "application/x-www-form-urlencoded") %>%
    req_body_form(
      grant_type = "refresh_token",
      client_id = client_creds$client_id,
      client_secret = client_creds$client_secret,
      refresh_token = refresh_token
    ) %>%
    req_perform()
  
  if (response$status_code != 200) {
    stop("Failed to get access token")
  }
  
  tokens <- yyjsonr::read_json_str(rawToChar(response$body))
  
  # Save to file and return
  saveRDS(tokens, ".secrets/.ts_tokens.rds")
  return(tokens)
} 