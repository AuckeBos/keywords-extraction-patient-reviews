# ==================================================================================================================== #
# Use the Keywords from keyword_extraction.R to perform sentiment analysis:
# - Use the scraper to add geographical locations to the providers
# - Parse the textual geographical locations to provinces
# - Provide functionality for a pie chart of the grades over all provinces
# - Provide functionality for a CSV that contains the keywords for all provinces in classes 'positive', 'negative'
#
# ==================================================================================================================== #

source('helpers.R')
library(magrittr)
library(stringr)
library(dplyr)
library(purrr)
library(tidyverse)
library(scales)
library(plotly)
# Geolocation library. Used public api to parse address info
library(nlgeocoder)
source('keyword_extraction.R')

#' Add location data by parsing the location string
#' @param providers The tibble of providers. Retrieved via scraper.R@get_providers(), updated with location string with add_locations_to_providers()
#' @return tibble providers, with columns containing location data added
add_location_data <- function(providers) {
  # Create empty vector
  result <- vector("list", length = nrow(providers))
  # Use 20 workers
  plan(multisession, workers = 20)
  # Batches of 200
  batch_size    <- 200
  start_indices <- seq(1, nrow(providers), batch_size)
  batch_count   <- length(start_indices)
  start_eta()
  for(i in seq_along(start_indices)) {
    start           <- start_indices[i]
    end             <- min(start + batch_size - 1, nrow(providers))
    current_indices <- start:end
    raw_locations   <- map(current_indices, function(row) { providers[row, 'location'] })

    # Save locations in list
    location_data <- future_map(seq_along(raw_locations), function(i) {
      raw_location    <- raw_locations[[i]]
      doc_id          <- current_indices[[i]]
      parsed_location <- nl_geocode(raw_location)
      # Return result as list, prepended with doc id
      return(list(doc_id, parsed_location))
    }, .options = furrr_options(seed = TRUE))
    # For all found locations, update result on right index
    if(length(location_data)) {
      for(parsed_data in location_data) {
        row             <- parsed_data[[1]]
        parsed_location <- parsed_data[[2]]
        result[[row]]   <- parsed_location
      }
    }
    eta(i, batch_count, "%s s per batch - ETA %s")
  }
  # Add vector to table
  providers$parsed_location                    <- result
  # Rename to prevent error on unnest wider due to double column names
  names(providers)[names(providers) == "type"] <- "provider_type"
  # Expand column
  providers %<>% unnest_wider('parsed_location')
  providers <- providers[!duplicated(providers$provider),]
  return(providers)
}

#' Create a pie chart for each province, showing the grade distributions
#' @param providers: The providers, containing parsed locations
#' @param reviews: Original review data
pie_plot_per_region <- function(providers, reviews) {
  reviews$grade_int <- reviews$grade %>% as.integer()
  # One row for each provider-grade_int, with col n_reviews
  reviews_sliced    <- reviews %>%
    group_by(provider, grade_int) %>%
    summarize('n_reviews' = n())
  # Join providers on the counts
  providers %<>% right_join(reviews_sliced)

  # One row for each region - grade_int. Weight is sum of n_reviews of all providers in the region
  providers_grouped <- providers %>%
    group_by(provincienaam, grade_int) %>%
    summarize(weight = sum(n_reviews))
  # Drop providers with unparsed adress
  providers_grouped <- providers_grouped[!is.na(providers_grouped$provincienaam),]

  # Pie chart
  blank_theme <- theme_minimal() +
    theme(
      axis.text.x  = element_blank(),
      axis.text.y  = element_blank(),
      axis.title.x = element_blank(),
      axis.title.y = element_blank(),
      panel.border = element_blank(),
      panel.grid   = element_blank(),
      axis.ticks   = element_blank(),
      plot.title   = element_text(size = 14, face = "bold")
    )

  ggplot(providers_grouped, aes(x = factor(1), y = weight, fill = grade_int %>% as.factor())) +
    geom_bar(stat = "identity", width = 1, position = position_fill()) +
    blank_theme +
    scale_x_discrete(NULL, expand = c(0, 0)) +
    scale_y_continuous(NULL, expand = c(0, 0)) +
    labs(fill = "Grade") +
    coord_polar(theta = "y") +
    facet_wrap(~provincienaam)
}

#' Create two csvs: One for positive, on for negative. Each csv shows the top ten keywords for each provincie within the grade class
#' @param data: Original review data
#' @param provider_type: The provider type to subset
#' @param providers: Tibble with providers, including parsed address string
get_scores_by_region_csv <- function(data, provider_type, providers) {
  # Subset 'provider_type' only
  data                    <- data[data$type == provider_type,]
  # Make sure review ids are sequential
  data$original_review_id <- data$review_id
  data$review_id          <- seq.int(nrow(data))
  # Add column provincienaam
  to_join                 <- providers[, c('provider', 'provincienaam')]
  data %<>% left_join(to_join, by = 'provider')
  # First set the category column on data. 1 category for each provincie-grade_class combination
  data %<>% set_category_column_on_data(c('provincienaam', 'grade_class'))
  # Get features
  features <- get_features(data)
  write_log('features computed and saved saved')

  # Features to DFM
  dfmat         <- get_reviews_dfm(data, features)
  fragments_dfm <- get_fragments_dfm(data, features)
  write_log('DFMs created and saved')

  # Compute scores
  scores <- features %>% add_scores(data, dfmat, fragments_dfm)
  write_log('Scores computed and saved')

  # Add col provincienaam on scores
  extra_data <- tibble('doc_id' = data$review_id, "provincienaam" = data$provincienaam)
  scores %<>% left_join(extra_data, by = "doc_id")

  # Drop doubles
  keywords <- drop_double_lemmas(scores, 'category')

  # 10 words per category
  top_words <- keywords %>%
    group_by(provincienaam, grade_class) %>%
    slice_max(final_score, n = 10, with_ties = FALSE) %>% # Top 10 per group
  { .[order(-.$final_score),] } # Order on final score
  # Drop unparsed provincies
  top_words <- top_words[!is.na(top_words$provincienaam),]

  # Format to two csvs
  top_words %<>% group_by(provincienaam) %>% nest()

  result_positive <- list()
  result_negative <- list()
  for(i in seq_along(top_words$provincienaam)) {
    name            <- top_words[i, 'provincienaam'] %>% toString()
    data            <- top_words[[i, 'data']][[1]]
    words_positive  <- data[data$grade_class == 1,]$lemma
    scores_positive <- data[data$grade_class == 1,]$final_score

    words_negative  <- data[data$grade_class == 0,]$lemma
    scores_negative <- data[data$grade_class == 0,]$final_score


    result_positive[[sprintf('%s keywords', name)]] <- words_positive
    result_positive[[sprintf('%s scores', name)]]   <- scores_positive

    result_negative[[sprintf('%s keywords', name)]] <- words_negative
    result_negative[[sprintf('%s scores', name)]]   <- scores_negative
  }
  result_positive %<>% as.tibble()
  result_negative %<>% as.tibble()
  write.csv(result_positive, sprintf('../data/evaluations/geo_results_%s_positive.csv', provider_type))
  write.csv(result_negative, sprintf('../data/evaluations/geo_results_%s_negative.csv', provider_type))
}

# Read providers from FS. created by scraper.R
providers <- readRDS('../data/providers_with_locations.RDS')
# Parse locations
providers %<>% add_location_data()
# Show pie chart for grade distributions along regions
pie_plot_per_region(providers, reviews)
# Generate CSV files with the keywords per province-gradeclass
get_scores_by_region_csv(reviews, 'Ziekenhuis ', providers)



