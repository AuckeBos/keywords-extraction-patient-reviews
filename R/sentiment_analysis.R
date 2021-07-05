# ==================================================================================================================== #
# Use the Keywords from keyword_extraction.R to perform sentiment analysis:
# - data$grade_class defines the general sentiment: <7 = negative, >=7 is positive
# - Define the category of a review based on the combination of the provider and the grade_class
# - Provide functionality to show the 10 top-keywords for each provider-class combination,
# - The resulting figure shows keywords for both classes for the provider
# ==================================================================================================================== #
source("keyword_extraction.R")

#' Plot the 10 highest scoring keywords in class 'positive' and 'negative'
#' @param scores: Frame with all scores
#' @param provider: The provider for which we want to plot the sentiment
#' @param data: The original review data
plot_sentiment_words <- function(scores, provider, data) {
  # Subset the scores
  scores <- scores[scores$provider == provider,] %>%
    group_by(lemma) %>%
    filter(n() > 3) # Only allow words that occur at least 3 times in all reviews of the provider

  # Drop double lemmas
  keywords <- drop_double_lemmas(scores, 'category')

  # Compute counts for both classes
  items     <- data[data$provider == provider,]
  pos_count <- sum(items$grade_class)
  neg_count <- nrow(items) - pos_count

  top_words <- keywords %>%
    group_by(grade_class) %>% # 2 groups
    slice_max(final_score, n = 10, with_ties = FALSE) %>% # Top 10 per group
  { .[order(-.$final_score),] } # Order on final score

  # # Add dependent words column
  top_words %<>% add_dependent_words(scores, 7)
  # # Plot words
  top_words %>%
    ggplot(aes(fct_reorder(paste(lemma, children, sep = "\n"), final_score), final_score, fill = grade_class)) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~grade_class, ncol = 2, scales = "free") +
    labs(x = "Final score", y = NULL, title = sprintf("Keywords for provider %s (%s documents of class 1, %s documents of class 0)", provider, pos_count, neg_count)) +
    coord_flip()
}

#' For a list of words, get all direct and indirect dependent words, according to the dependency tree
#' A word is dependent on a word if it occurs below the word in the dependency tree. Eg traverse down the trees of head_ids, return all children
#' @param sentence_features: Subset of 'scores', containing all rows of the sentence for which we are computing dependencies
#' @param head_ids: The ids for which we want to find the dependencies
dependencies_within_sentence <- function(sentence_features, head_ids) {
  # Compute all direct children. Eg features where head_token_id in head_ids
  children     <- sentence_features[sentence_features$head_token_id %in% head_ids &
    sentence_features$token_id %in% head_ids %>% `!` # Exclude the head id itself
    ,]
  # & sentence_features$pos %in% c('ADJ', 'ADV'),] # Only allow Adjectives and Adverbs
  children_ids <- children$token_id
  if(length(children_ids) > 0) { # Also retrieve children of children, recursively
    children <- rbind(children, dependencies_within_sentence(sentence_features, children_ids))
  }
  # Children now contains all words occuring below head_ids in the dependency tree
  return(children)
}

#' Add a column 'children' to 'words'. This column holds the n highest scoring words that occur as a
#' dependency in the dependency tree of the sentences
#' @param words: The words to add the dependencies to. Is a subset of 'scores', eg each 'word' is a row in the scores matrix
#' @param scores: The complete scores matrix
#' @param n: The number of children to add per word
#' @return: words, with column added
add_dependent_words <- function(words, scores, n = 3) {
  # Compute children words for each word
  children       <- apply(words, 1, function(row) {
    word           <- row['lemma'] %>% as.character()
    grade_class    <- row['grade_class'] %>% as.character()
    # Find all occurences of this word inside 'scores', within the same grade_class
    all_occurences <- scores[scores$lemma == word & scores$grade_class == grade_class,]
    # For each occurence, find the direct and indirect children
    children       <- apply(all_occurences, 1, function(occurence) {
      doc_id            <- occurence[['doc_id']] %>% as.numeric()
      sentence_id       <- occurence[['sentence_id']] %>% as.numeric()
      token_id          <- occurence[['token_id']] %>% as.numeric()
      # sentence_features are all features of the sentence in which this occurence is found
      sentence_features <- scores[scores$doc_id == doc_id & scores$sentence_id == sentence_id,]
      # Compute the children
      children          <- dependencies_within_sentence(sentence_features, token_id)
      return(children)
    }) %>% rbindlist()
    # Take only the top three children for this word
    top_n          <- children %>%
      group_by(lemma) %>%
      slice_max(final_score, n = 1, with_ties = FALSE) %>% # If a lemma occurs more than once as child, take only the highest-scoring one
      ungroup() %>%
      slice_max(final_score, n = n, with_ties = FALSE) # Take the three highest scoring children
    return(sprintf("(%s)", top_n$lemma %>% toString())) # Return as string
  })
  # Add result as column, return
  words$children <- children
  return(words)
}

keyword_extraction.init()

# Set category column based on combination of provider and grade class
data %<>% set_category_column_on_data(c("provider", "grade_class"))
# Compute scores based on new classes
scores_for_sentiment <- features %>% add_scores(data, dfmat, fragments_dfm)

# Only providers with at least 100 reviews in both classes
provider_options <- data %>%
  group_by(provider, grade_class) %>%
  filter(n() >= 100) %>% # Keep rows where more than 25 in provider-class
  slice(1) %>% # One row for each group
  group_by(provider) %>%
  filter(n() == 2) %>% # Now the providers with 2 groups (postivive, negative) have >= 25 providers
{ unique(.$provider) }

# Sample a random provider
provider <- sample(provider_options, 1)

# Plot the result
plot_sentiment_words(scores_cat_grade, provider, data)