# Import libs
# Used to scrape web pages
library(httr)
library(rvest)
# Used for multi-core scraping
library(furrr)
# Import helper functions
library(magrittr)
source("helpers.R")

# ==================================================================================================================== #
# FUNCTIONALITY:
# Scrapes all reviews from ZorgkaartNederland, using the following steps
#
# 1. Load all Organisation types from /overzicht/organisatietypes. For each, save the name and url
# 2. For each type, Load all existing providers of that type. For each, save the name, type, and url
# 3. For each provider, Load all reviews w/o details. E.g. loop over the list of reviews, saving:
#   - The url to the review, will be used to retrieve additional info
#   - The date
#   - The average grade
#   - The type of the organisation
#   - The name of the organisation
#   We exclude details, because we can get above info 20 reviews at a time, without loading the url
# 4. For each review, open the url, and retrieve additional info:
#   - The grade distribution: The names and its values for all grades that are provided
#   - The review text
#   - The department where the reviewer went, or NA
#   - The condition for which the reviewer went to the provider, or NA
#
# Results are saved in a tibble, and written to RDS and csv

# Base url of zorgkaart
base_url <- "https://www.zorgkaartnederland.nl"

# ==================================================================================================================== #
# HELPER FUNCTIONS
# ==================================================================================================================== #

#' Load the page of an url, or return Nan on failure
#' @param url: The url to load
#' @param timeout: The max timeout
#' @return: The html or nan
load_or_na <- function(url, timeout = 3) {
  result <- tryCatch({
    GET(url, timeout(timeout)) %>% read_html()
  }, error = function(e) {
    write_log(sprintf("Could not load page %s, skipping...", url))
    return(NA)
  })
  return(result)
}

#' Process a list of urls async: For each url, run the callback(). Process with num_workers workers
#' @param urls: The list of urls to process
#' @param callback: The callback to apply to the page of each url, should consume 'page'
#' @param provide_iterator_to_callback: If true, call callback with params (page, index) instead of (page)
#' @param timeout: The timeout on page load
#' @return: Flattened results. Eg the result of flatten(result of callback(url) for url in urls)
process_urls_async <- function(urls, callback, provide_iterator_to_callback = FALSE, timeout = 6) {
  # Asynchronously process the urls
  result <- future_map(seq_along(urls), function(i) {
    url  <- urls[[i]]
    page <- load_or_na(url, timeout)
    if(is.na(page)) {
      return(NA)
    }
    if(provide_iterator_to_callback) {
      return(callback(page, i))
    }else {
      return(callback(page))
    }
  })
  # Drop na
  result <- result[!is.na(result)]
  if(!length(result)) {
    return(list())
  }
  # Convert to matrix
  result %<>% { do.call("rbind", .) }
  # Convert to list of lists: Each col is a row
  result %<>% { map(1:ncol(.), function(i) {
    col <- .[, i]
    # If the values of the columns are not named lists, unlist the column
    if(length(col) && is.null(names(col[[1]]))) {
      col %<>% unlist()
    }

    return(col)
  })
  }


  return(result)
}

#' Get the max page number, extracted from the pagination ul. Applicable on many pages on Zorgkaart
#' @param page: The html of the page
#' @param page_is_url: If TRUE, 'page' is not HTML, but actually the URL.
#' @result: The value of max_page, as extracted from ul.pagination. Or NAN if not found
get_max_page <- function(page, page_is_url = FALSE) {
  if(page_is_url) {
    page %<>% load_or_na()
  }
  if(is.na(page)) {
    return(NA)
  }
  return(page %>%
    rvest::html_node("ul.pagination > li:last-child a") %>%
    html_text() %>%
    strtoi())
}

# ==================================================================================================================== #
# LET THE SCRAPING BEGIN
# ==================================================================================================================== #

#' Get a list of provider types
#' @return DF of provider types
get_provider_types <- function() {
  write_log("Retrieving organisations")
  # Load organisation types
  page <- sprintf("%s/overzicht/organisatietypes", base_url) %>% load_or_na()

  # Load rows by <a>'s
  rows   <- page %>% rvest::html_nodes('a.filter-radio')
  result <- tibble(
    "type" = rows %>%
      rvest::html_text() %>%
    { sub("\\(.*", "", .) },
    "url"  = rows %>% rvest::html_attr("href")
  )

  return(result)
}

#' Get al providers of a specific provider type
#' @param organisation.url: The url of the organisation
#' @return: List of 2 columns: names, urls
get_providers_of_type <- function(organisation.url) {
  # Load max page by pagination selector
  max_page <- sprintf("%s%s", base_url, organisation.url) %>% get_max_page(TRUE)
  if(is.na(max_page)) { # If no page found (only 1 page of providers), use current page
    urls <- c(sprintf("%s%s", base_url, organisation.url))
  }else { # Else, will iterate over all pages
    urls <- map(1:max_page, function(page) { sprintf("%s%s/pagina%s", base_url, organisation.url, page) })
  }

  info <- process_urls_async(urls, function(page) {
    current.names <- page %>%
      html_nodes("a.filter-result__name") %>%
      html_text() %>%
      str_trim()
    current.urls  <- page %>%
      html_nodes("a.filter-result__name") %>%
      html_attr("href") %>%
      str_trim()
    return(list(current.names, current.urls))
  })
  # Format of info: [names, urls]
  return(info)
}

#'Get all providers for all organisations in get_organisations
#' @return: DF of providers, containing provider, type, url
get_providers <- function() {
  types <- get_provider_types()
  write_log("Retrieving providers")
  providers.types <- c()
  providers.names <- c()
  providers.urls  <- c()
  count           <- nrow(types)
  start_eta()
  for(row in 1:count) {
    type              <- toString(types[row, 'type'])
    type.url          <- toString(types[row, 'url'])
    # Format [names, urls]
    providers.current <- get_providers_of_type(type.url)
    if(!length(providers.current)) {
      next
    }
    providers.types <- c(providers.types, rep(type, length(providers.current[[1]])))
    providers.names <- c(providers.names, providers.current[[1]])
    providers.urls  <- c(providers.urls, providers.current[[2]])
    eta(row, count, "%s s per provider type - ETA %s")
  }

  # To Frame
  df <- tibble("provider" = providers.names, "type" = providers.types, "url" = providers.urls)

  return(df)
}

#' Add a column 'location' to the providers table. Contains the raw string location as scraped from ZKN
#' @param providers: The providers, as retrieved by get_providers
#' @return providers, column 'location' added
add_locations_to_providers <- function(providers) {
  # Add empty col
  providers$location <- c('')
  # Batches of 200
  batch_size         <- 200
  start_indices <- seq(1, nrow(providers), batch_size)
  batch_count   <- length(start_indices)
  start_eta()
  for(i in seq_along(start_indices)) {
    start           <- start_indices[i]
    end             <- min(start + batch_size - 1, nrow(providers))
    current_indices <- start:end
    urls            <- map(current_indices, function(row) { sprintf("%s%s", base_url, providers[row, 'url']) })
    # Save locations in list
    locations       <- process_urls_async(urls, function(page, index) {
      # Load all rows from the box. Contains provider info. One of the rows contains the address. load by label
      options <- page %>% html_nodes("div.box--rounded div.row")
      for(row in options) {
        label <- row %>%
          html_node(":first-child") %>%
          html_text() %>%
          str_trim()
        value <- row %>%
          html_node(":last-child") %>%
          html_text() %>%
          str_trim()
        # If found, set column value
        if(grepl("Adres", label, fixed = TRUE)) {
          # Return list, prepended with reviwe id
          return(list(current_indices[[index]], value))
          break
        }
      }
      return(list())
    }, TRUE)
    # For all found locations, update location column on providers tibble
    if(length(locations)) {
      for(iter in 1:length(locations[[1]])) {
        row                        <- locations[[1]][[iter]]
        location                   <- locations[[2]][[iter]]
        providers[row, 'location'] <- location
      }
    }
    eta(i, batch_count, "%s s per batch - ETA %s")
  }
  return(providers)

}

#' Get all reviews for all providers. Skip details, such that we do not need to open a page for each review
#' @param providers: DF of providers.
#' @return: DF of reviews, containing provider, name, type, grade, date, url
get_reviews <- function(providers) {
  write_log('Retrieving reviews w/o details')
  grades      <- c()
  dates       <- c()
  names       <- c()
  types       <- c()
  urls        <- c()
  batch_count <- nrow(providers)
  start_eta()
  for(row in 1:batch_count) {
    provider     <- toString(providers[row, "provider"])
    provider.url <- toString(providers[row, 'url'])
    type         <- toString(providers[row, 'type'])

    page <- sprintf("%s%s/waardering", base_url, provider.url) %>% load_or_na()

    # If error occurs, the /waarderingen page doesn't exist, so either
    # A) No reviews have been written
    # B) The provider is a group of providers (it has more than 1 location). Thus skip it, locations will be handled later
    if(is.na(page)) {
      next
    }

    max_page <- get_max_page(page)
    if(is.na(max_page)) { # If no page found (only 1 page of reviews), use current page
      urls.for.provider <- c(sprintf("%s%s/waardering", base_url, provider.url))
    }else {
      urls.for.provider <- map(1:max_page, function(page) { sprintf("%s%s/waardering/pagina%s", base_url, provider.url, page) })
    }
    info <- process_urls_async(urls.for.provider, function(page) {
      grades.current <- page %>%
        html_nodes("ul.results_holder li div.fractional_number") %>%
        html_text() %>%
        str_trim() %>%
        as.numeric()
      dates.current  <- page %>%
        html_nodes("ul.results_holder li h4.title a") %>%
        html_text() %>%
        str_trim() %>%
      { sub(" - Goedgekeurd door de redactie.*", "", .) } %>%
        lubridate::dmy()
      urls.current   <- page %>%
        html_nodes("ul.results_holder li h4.title a") %>%
        html_attr("href")
      return(list(grades.current, dates.current, urls.current))
    }, FALSE, 20)
    if(length(info)) {
      # Format: [grades, dates, urls]. Extend existing data
      grades <- c(grades, info[[1]])
      dates  <- c(dates, info[[2]])
      urls   <- c(urls, info[[3]])
      names  <- c(names, rep(provider, length(info[[1]])))
      types  <- c(types, rep(type, length(info[[1]])))
    }
    eta(row, batch_count, "%s s per provider - ETA %s")
  }

  # To Frame
  df <- tibble("provider" = names, "type" = types, "grade" = grades, "date" = as.Date(dates, origin = "1970-01-01"), "url" = urls)
  return(df)
}

#' Get the details of a single review: The grade distribution, text, and possible condition and department
#' @param page: The page of the review
#' @return: [grades, text, condition, department]
get_detail_of_review <- function(page) {
  grades.values    <- page %>%
    html_nodes("ul.sliders_list li div.fractional_number") %>%
    html_text() %>%
    str_trim() %>%
    as.numeric()
  grades.catgories <- page %>%
    html_nodes("ul.sliders_list li div label.title") %>%
    html_text() %>%
    str_trim()
  grades           <- grades.values
  names(grades)    <- grades.catgories
  text             <- page %>%
    html_node("div.sliders_holder.form_holder div.with_left_margin.with_right_margin >div:last-child") %>%
    html_text() %>%
    str_trim()
  # Extra info might hold 'Condition' and 'Department'. We can only select these via the html text of the labels
  extra_info       <- page %>%
    html_nodes("div.sliders_holder.form_holder ul.rating_holder_small li")
  condition        <- NA
  department       <- NA
  map(extra_info, function(li) {
    label <- li %>%
      html_node("div.left_media_holder label") %>%
      html_text() %>%
      str_trim()
    value <- li %>%
      html_node("div.media-body") %>%
      html_text() %>%
      str_trim()
    if(grepl("Aandoening", label, fixed = TRUE)) {
      condition <<- value
    }else if(grepl("Specialisme", label, fixed = TRUE)) {
      department <<- value
    }
  })
  return(list(grades, text, condition, department))
}

#' Add the details of the reviews to the DF
#' Use batches of 200 reviews per batch
#' For each batch, retrieve the details async with 25 workers
#' @param reviews: The reviews without details
#' @return: The reviews with details, eg with columns 'grade_distribution', 'text', 'condition', and 'department' are added
add_details_to_reviews <- function(reviews) {
  write_log('Retrieving review details')

  # Create emtpy vectors
  grade_distributions <- vector("list", length = nrow(reviews))
  texts               <- vector("character", length = nrow(reviews))
  conditions          <- vector("character", length = nrow(reviews))
  departments         <- vector("character", length = nrow(reviews))

  # Batch the details into 200 pages per batch, use 25 workers to crawl
  plan(multisession, workers = 25)
  batch_size <- 200

  start_indices <- seq(1, nrow(reviews), batch_size)
  batch_count   <- length(start_indices)
  start_eta()
  for(i in seq_along(start_indices)) {
    start           <- start_indices[i]
    end             <- min(start + batch_size - 1, nrow(reviews))
    current_indices <- start:end
    urls            <- map(current_indices, function(row) { sprintf("%s%s", base_url, reviews[row, 'url']) })
    details         <- process_urls_async(urls, function(page, index) {
      # Prepend row index
      row <- current_indices[[index]]
      page %>%
        get_detail_of_review() %>%
        prepend(row) %>%
        return()
    }, TRUE)
    if(length(details)) {
      for(iter in 1:length(details[[1]])) {
        row                <- details[[1]][[iter]]
        grade_distribution <- details[[2]][[iter]]
        text               <- details[[3]][[iter]]
        condition          <- details[[4]][[iter]]
        department         <- details[[5]][[iter]]

        grade_distributions[[row]] <- grade_distribution
        texts[row]                 <- text
        conditions[row]            <- condition
        departments[row]           <- department
      }
    }
    eta(i, batch_count, "%s s per batch - ETA %s")
  }
  # Add vectors to DF
  reviews$grades     <- grade_distributions
  reviews$text       <- texts
  reviews$condition  <- conditions
  reviews$department <- departments
  return(reviews)
}

#' Run the scraper:
#' - Get all providers for all provider types
#' - For each provider, load all reviews, w/o the details (eg w/o grade distribution, text, condition and department)
#' - For each review, load the details
#' - Save data to RDS and csv
#'
#' Note: Num workers is 10 for get_providers and get_reviews. add_details_to_reviews Sets num workers to 25
scraper.run <- function() {
  # Use 10 workers to crawl
  plan(multisession, workers = 20)

  providers            <<- get_providers()
  # If you want to load the locations of the providers and save to RDS, uncomment below two lines
  # providers_with_locations <- add_locations_to_providers(providers)
  # saveRDS(providers_with_locations, '../data/providers_with_locations.RDS')

  reviews              <- get_reviews(providers)
  reviews_with_details <- add_details_to_reviews(reviews)

  # Save to filesystem

  # saveRDS(reviews_with_details, '../data/reviews_nested.RDS')

  # Unnest grade_distribution: Each grade category gets its own column
  reviews_with_details_unnested <- unnest_wider(reviews_with_details, 'grades', names_sep = '_')

  # Save to RDS and csv
  # saveRDS(reviews_with_details_unnested, '../data/reviews.RDS')
  # write.csv(reviews_with_details_unnested, '../data/reviews.csv')
}

scraper.run()
