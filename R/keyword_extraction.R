# Collection of tidy packages
library(tidyverse)
library(tidymodels)
library(tidytext)
library(data.table)
# Text mining libs
# For DFM
library(quanteda)
# For text parsing
library(spacyr)
# [DEPRECATED] for evaluate_keywords
library(RTextTools)
# Some helper functions
library(Matrix)
library(magrittr)
# For confusion matrix
library(caret)
# toJSON
library(rjson)

# Helper functions
source("helpers.R")
# ==================================================================================================================== #
# Keyword extraction using the paper of Timonen et al:
# Informativeness-based Keyword Extraction from Short Documents
# https://www.researchgate.net/publication/269280274
# Technical details can be found in the Report
#
# E.g.
# 1. Load the complete dataset: one row per review. Created via the scraper
# 2. Generate 'features': One row for each word in each review (Tidy), contains several features like pos tag, pos
#     weight, position in sentence,  lemma, etc. Note that we use lemmas as terms in the rest of the algorithm.
# 3. Create DFMs on two levels:
#     - dfmat is the document frequency matrix on the level of a review: on row for each review
#     - fragments_dfm is the matrix on the level of a fragment (Section 3.3.2 of the paper). Use to compute TCoR
# 4. Remove stopwords, spaces and punctuation
# 5. Compute the score for each word-category combination, as in the paper, using above DFMs
# 6. Subset only those words that are keywords: At most 10 per review, with a score of at least .5 * the max score of
#     the review
#
#
# There are several plotting function and hyperparameter-tuning function available as well
# ==================================================================================================================== #


# ==================================================================================================================== #
# HELPER FUNCTIONS
# ==================================================================================================================== #

#' Defines the weight for a POS tag. POS tags not in this list have a weight of 0.1.
#' @return: List that maps POS tags to their weight
pos_weights <- function() {
  # New, determined by evaluate_posweights
  return(list(
    "NOUN" = 1,
    "VERB" = .5,
    "ADJ"  = 1,
    "ADV"  = .5
  ))
}

#' Convert a DFM to tidy format: One row for document-term.
#' Rename columns
#' @param dfmat: The DFM
#' @return: The DFM in tidy format (tibble with columns document, term, count)
dfm.tidy <- function(dfmat) {
  frame        <- tidy(dfmat)
  names(frame) <- c('document', 'term', 'count')
  return(frame)
}

#' [Deprecated] Instead of squishing, we now use log transformation on the category score
#'
#' Squish a column. This means we push all values outside a certain percentile, to that percentile.
#' For some scores, we noticed that pure normalization results in very low values, since almost all values are
#' < 0.01, and some values are > 1. Squishing high values before normalization makes sure the distribution of the column is better distributed.
#' The few values that are > 1 are squised to the percentile, and after normalisation the values will be more evenly distributed accross [0, 1]
#' If this is not done, category_score << corpus_score in almost all cases, thus term_score is dependent only on corpus_score
#' @param column: The column with the values to be squished
#' @param min_percentile: Percentile to use for min squashing: all values below this percentile are set to the percentile
#' @param max_percentile: Percentile to use for max squashing: all values above this percentile are set to the percentile
#' @return: Squished column
squish_column <- function(column, min_percentile = NA, max_percentile = NA) {
  if(!is.na(min_percentile)) {
    min_value                  <- quantile(column, c(min_percentile))
    column[column < min_value] <- min_value
  }
  if(!is.na(max_percentile)) {
    max_value                  <- quantile(column, c(max_percentile))
    column[column > max_value] <- max_value
  }

  return(column)
}

#' Normalize a column of a frame to [0, 1], via min-max normalization
#' @param column: The column to normalize
#' @param handle_infs: If true, handle infinite values. In that case, exclude infinite values when calculating max/min, and set those to 0 afterwards
#' @return The frame with the column normalized
normalize <- function(column, handle_infs = FALSE) {
  if(handle_infs) {
    valid   <- column[is.finite(column)]
    max_val <- max(valid)
    min_val <- min(valid)
  }else {
    max_val <- max(column)
    min_val <- min(column)
  }
  normalized <- (column - min_val) / (max_val - min_val)
  if(handle_infs) {
    normalized[!is.finite(normalized)] <- 0
  }
  return(normalized)
}

#' Plot the distribution of IDF FW for different values of alpha
#' @param features: The features DFM
plot_corpus_score_distribution_for_differet_alphas <- function(features) {
  pl <- ggplot() +
    geom_line() +
    labs(x = "IDF FW", y = "Density", title = "Corpus score densities, normalized over the whole corpus")
  for(a in c(0.5, 0.9, 1, 1.1, 1.5, 2)) {
    score        <- get_corpus_score(dfmat, a)
    features_cur <- features
    features_cur %<>% left_join(score, by = c("lemma" = "term"))
    features_cur$corpus_score[is.na(features_cur$corpus_score)] <- 0
    features_cur$corpus_score %<>% normalize()
    features_cur$corpus_score[is.na(features_cur$corpus_score)] <- 0
    distr                                                       <- density(features_cur$corpus_score)
    x                                                           <- distr[['x']]
    y                                                           <- distr[['y']]
    data                                                        <- tibble("x" = x, "y" = y, "a" = as.factor(a))
    pl                                                          <- pl + geom_line(data, mapping = aes(x = x, y = y, color = a))
    write_log(sprintf("Density computation for alpha=%s done", a))
  }
  print(pl)
}

#' Plot the distribution of IDF FW for different values of n_0
#' @param features: The features DFM
plot_corpus_score_distribution_for_differet_n <- function(features) {
  pl <- ggplot() +
    geom_line() +
    labs(x = "IDF FW", y = "Density", title = "Corpus score densities, normalized over the whole corpus")
  for(n in c(0.01, 0.03, 0.05, 0.08, 0.1, 0.2)) {
    score        <- get_corpus_score(dfmat, n = n)
    features_cur <- features
    features_cur %<>% left_join(score, by = c("lemma" = "term"))
    features_cur$corpus_score[is.na(features_cur$corpus_score)] <- 0
    features_cur$corpus_score %<>% normalize()
    features_cur$corpus_score[is.na(features_cur$corpus_score)] <- 0
    distr                                                       <- density(features_cur$corpus_score)
    x                                                           <- distr[['x']]
    y                                                           <- distr[['y']]
    data                                                        <- tibble("x" = x, "y" = y, "n" = as.factor(n))
    pl                                                          <- pl + geom_line(data, mapping = aes(x = x, y = y, color = n))
    write_log(sprintf("Density computation for n0=%s done", n))
  }
  print(pl)
}

#' [DEPRECATED]
#' Evaluate keyword performance by using them to classifiy reviews
#' Select some models, try to classify the reviews into positive (>=7)/negative, using the keywords.
#' @param keywords: The keyword matrix: One row for each doc-keyword. Eg the 'features' with the scores added
#' @param data: The original data, eg one row per review
#' @return list(results, analytics)
evaluate_keywords <- function(keywords, data) {
  # Reproduction
  set.seed(555)
  # Sample 1000 reviews of each class
  selected_ids <- (data %>% group_by(grade_class) %>% sample_n(1000))$review_id
  selected     <- keywords[keywords$doc_id %in% selected_ids,]
  # Reformat from Tidy to DFM
  mat          <- cast_dfm(selected, doc_id, lemma, final_score)

  # Train test split
  train_ids  <- sample(seq_len(nrow(mat)), .9 * nrow(mat))
  train_data <- mat[train_ids,]
  test_data  <- mat[-train_ids,]

  train_ids <- docid(train_data) %>%
    as.character() %>%
    as.integer()
  test_ids  <- docid(test_data) %>%
    as.character() %>%
    as.integer()

  # Extract labels from data
  train_labels <- data[train_ids,]$grade_class

  # Train val split
  train_size <- 1:(0.8 * nrow(train_data))
  val_size   <- floor(0.8 * nrow(train_data) + 1):nrow(train_data)

  train_container <- create_container(train_data, train_labels, train_size, val_size, virgin = FALSE)
  # models          <- train_models(train_container, algorithms = c("BAGGING", "BOOSTING", "GLMNET", "RF", "SLDA", "SVM", "TREE"))
  models          <- train_models(train_container, algorithms = c("RF", "SVM"))

  # Test set
  test_labels    <- data[test_ids,]$grade_class
  test_container <- create_container(test_data, test_labels, testSize = 1:length(test_labels), virgin = FALSE)
  results        <- classify_models(test_container, models)

  analytics     <- create_analytics(test_container, results)
  results$LABEL <- test_labels

  # Best scoring models:
  # Random Forest: F1 of 0.71
  # SVM: F1 of 0.69
  return(list(results, analytics))
  # mat <- confusionMatrix(as.factor(results$SVM_LABEL), as.factor(results$LABEL))
}

#' Set a column 'category' on the data. This column defines the category of a review. It is coppied
#' to the other data structures: dfms and features. The column value is set using group_indices(based_on)
#' @param data: The original data, eg one row per review
#' @param based_on: list of columns that define the category. For example if this list is c('provider') each unique provider
#'  gets its own category. if the list is c('provider', 'grade_class'), each unique provider-grade_class combination gets its own category
#' @return: data, with column 'category' added
set_category_column_on_data <- function(data, based_on = c('provider')) {
  # Convert vector of strings to vector of symbols, needed for group_indices
  based_on %<>% lapply(as.symbol)
  data$category <- data %>% group_indices(.dots = based_on)
  return(data)
}

#' Compute the F1 score for scores on the testset
#' @param scores: The scored features
#' @param testset: The testset. - Tibble with column 'review_id' and 'keyword', one row per keyword
#' @return: list with f1, precision, recall, and scores per-review
compute_f1 <- function(scores, testset) {
  # Subset only those reviews that we will test
  scores %<>% filter(doc_id %in% unique(testset$review_id))
  # Scores to keywords
  keywords <- get_keywords(scores)
  result   <- keywords %>%
    group_by(doc_id) %>% # Compute F1 per-review
    group_modify(function(current_keywords, review_id) {
      review_id %<>% as.character() %>% as.integer()
      # Get labels from the testset, predictions from the keywords
      labels      <- testset[testset$review_id == review_id,]$keyword
      predictions <- unique(current_keywords$lemma)

      # Count of words that are in the predictions and the labels
      true_positive_count <- length(intersect(labels, predictions))

      # Compute F1
      precision <- true_positive_count / length(predictions)
      recall    <- true_positive_count / length(labels)
      beta      <- 1
      f1        <- (1 + beta^2) * (precision * recall) / (beta^2 * (precision + recall))
      doclength <- current_keywords[[1, 'doc_length']]
      if(is.na(doclength)) {
        doclength <- 1
      }

      return(tibble("f1" = f1, "precision" = precision, "recall" = recall, "length" = doclength, "predictions" = list(predictions), "labels" = list(labels)))
    })
  is.result$f1 %<>% as.numeric()
  result$precision %<>% as.numeric()
  result$recall %<>% as.numeric()
  result %<>% replace(is.na(result), 0)
  return(
    list(
      "precision"  = mean(result$precision),
      "recall"     = mean(result$recall),
      "f1"         = mean(result$f1),
      "per_review" = result
    )
  )
}

#' Evaluate different combinations of the pos_weights array. Compute scores with different weights,
#' evaluate on both versions of the testset the testset. Return results
#' @param features: The features matrix
#' @param data: The original data
#' @param dfmat: The dfm
#' @param fragments_dfm: The dfm on fragment level
#' @return list of two tibbles, one for each testset. Tibbles contain results
evaluate_pos_weights <- function(features, data, dfmat, fragments_dfm) {
  # Define pos weight options
  pos_weight_options <- list(
    list(
      "NOUN" = 1,
      "VERB" = .8,
      "ADJ"  = .8,
      "ADV"  = .8
    ), list(
      "NOUN" = 1,
      "VERB" = 1,
      "ADJ"  = 1,
      "ADV"  = 1
    ), list(
      "NOUN" = 1,
      "VERB" = .5,
      "ADJ"  = .8,
      "ADV"  = .7
    ), list(
      "NOUN"  = 1,
      "VERB"  = 1,
      "ADJ"   = .8,
      "ADV"   = .8,
      "PROPN" = .6
    ), list( # This options shows best result
      "NOUN" = 1,
      "VERB" = .5,
      "ADJ"  = 1,
      "ADV"  = .5
    ),
    list(
      "NOUN" = 1,
      "VERB" = .5,
      "ADJ"  = 0.8,
      "ADV"  = .25
    ),
    list(
      "NOUN" = 1,
      "VERB" = .5,
      "ADJ"  = 0.8,
      "ADV"  = .25
    )
  )
  # Best hyperparameter values, based on evaluate_hyperparameters
  a                  <- 1.5
  n                  <- 0.1
  b                  <- 0.75

  # Add column 'category' on features
  features$category <- data[features$doc_id,]$category

  # As in add_scores: first clean features features
  # Drop PUNCT, SPACE and stopwords
  features %<>% { .[.$pos %in% c('PUNCT', 'SPACE') %>% `!` & !.$is_stop,] }

  # Drop pos scores
  features %<>% subset(select = -c(pos_score))

  # Create doccano test sets
  doccano_results <- parse_doccano('../data/doccano', features, TRUE)
  testset_loose   <- doccano_results[[2]]
  doccano_results <- parse_doccano('../data/doccano', features, FALSE)
  testset_strict  <- doccano_results[[2]]
  results_loose   <- list()
  results_strict  <- list()

  category_scores <- get_category_score(data, dfmat, fragments_dfm)
  corpus_scores   <- get_corpus_score(dfmat, a, n)
  scores          <- left_join(category_scores, corpus_scores, by = "term")

  # Add Category and Corpus scores
  features %<>% left_join(scores, by = c("lemma" = "term", "category" = "category"))

  # Nan to 0, else normalisation doesn't work
  features$corpus_score[is.na(features$corpus_score)]     <- 0
  features$category_score[is.na(features$category_score)] <- 0

  # Remove all words of which the lemma occurs <=2 times in the complete corpus. These are so rare that they should not be taken into account
  features %<>% group_by(lemma) %>% filter(n() > 2)

  # normalize corp score over whole corpus
  features$corpus_score %<>% normalize()
  # Normalize cat score over whole corpus, apply log transformation first
  features$category_score %<>% log() %>% normalize(TRUE)
  features$term_score <- ((b * features$category_score) + ((1 - b) * features$corpus_score)) / 2

  batch_count <- length(pos_weight_options)
  i           <- 1
  for(pos_weights in pos_weight_options) {
    features_current                                              <- features
    # Set pos weights
    features_current$pos_score                                    <- pos_weights[features_current$pos] %>%
      as.character() %>%
      as.numeric()
    # Different from the paper. If the POS-tag has no weight, assign score of .1 instead of 0. If spacy incorrectly parsed
    # the word, we allow for the possibility that the word is still relevant, although minor
    features_current$pos_score[is.na(features_current$pos_score)] <- .1

    # Compute final score. Do not use index score (best hyperparameter option)
    features_current$final_score                                      <- features_current$term_score * features_current$pos_score
    # Nan to 0
    features_current$final_score[is.na(features_current$final_score)] <- 0

    result_loose  <- compute_f1(features_current, testset_loose)
    result_strict <- compute_f1(features_current, testset_strict)

    result_loose %<>% append(list(
      "pos_weights" = pos_weights
    ))
    result_strict %<>% append(list(
      "pos_weights" = pos_weights
    ))

    results_loose %<>% rbind(result_loose)
    results_strict %<>% rbind(result_strict)
    eta(i, batch_count, "%s per configuration, done at %s")
    i <- i + 1
  }
  results_loose %<>% as.data.frame()
  results_strict %<>% as.data.frame()
  return(list(results_loose, results_strict))
}


#' Try different versions of several hyperparameters:
#' - a is the alpha in Equation 3 of the paper. It influences the penalty for low-occuring words: those where the term-frequency equals
#'      the document frequency, and this frequency is below n_0.
#' - n is the n_0 value of Equation 3. Or actually the percentage used to calculate n_0. This value indicates the percentage
#'      of documents a wordt should occur in, to achieve an optimal corpus score
#' - b is the beta value of Equation 10. It defines the weight of the category_score vs the weight of hte corpus_score. .5 is a harmonic mean
#' - corp_normalizations_per_doc is a boolean. If true, the corpus scores are normalized within each doc, eg for each document (review) the word
#'      with the highest corpus_score has a corpus_score of one. If false, we normalize over the whole corpus. Eg the word with the highest corpus
#'      score in the complete corpus, will have a value of 1.
#' - category_normalizations_per_category is a boolean. If true, the category scores are normalized within each category, eg for each category (provider) the word
#'      with the highest category_score has a category_score of one. If false, we normalize over the whole corpus. Eg the word with the highest category_score
#'      in the complete corpus, will have a value of 1.
#' - use_index_scores is a boolean. If true, final_score = term_score * pos_score * index_score. If false, final_score = term_score * pos_score.
#'      It is unclear whether index_score increases performance, thus we add it to the configuration
#'
#' For all combinations of different values, calculate the corpus score (category score is not affected by these), and with that the final score.
#'      With the final score compute the keywords, and compute teh F1 of those keywords wrt the doccano test set. The result is a tibble
#'      with one row for each combination of parameters, and columns f1, precision and recall, which indicate the mean of those values.
#'      We  compute scores on two versions of the testset: loose and strict.
#' @param features: the features, uncleaned, without scores
#' @param data: the original review data, one row per review. A column 'category' should have been added using set_category_column_on_data
#' @param dfmat: The DFM
#' @param fragments_dfm: The DFM on fragment level
#' @return: List of two tibbles: Results on the loose testet, and those on the strict testset.
#'          Tibbles have one row for each hyperparameter combination, containing avg f1, precision and recall
evaluate_hyperparameters <- function(features, data, dfmat, fragments_dfm) {
  # Add column 'category' on features
  features$category <- data[features$doc_id,]$category

  # AS IN add_scores: FIRST CLEAN features
  # Drop PUNCT, SPACE and stopwords
  features %<>% { .[.$pos %in% c('PUNCT', 'SPACE') %>% `!` & !.$is_stop,] }

  # Equation 11: Add index_score via doc_length and feature_index - d(t) as in 3.3.3 of the paper.
  features %<>% group_by(doc_id) %>% add_tally(name = "doc_length")
  features %<>% group_by(doc_id) %>% mutate(feature_index = seq.int(n()) - 1)
  features$index_score <- 1 - features$feature_index / features$doc_length

  # Create doccano test set, loose and strict
  doccano_results <- parse_doccano('../data/doccano', features, TRUE)
  testset_loose   <- doccano_results[[2]]

  doccano_results <- parse_doccano('../data/doccano', features, FALSE)
  testset_strict  <- doccano_results[[2]]

  results_loose  <- list()
  results_strict <- list()

  category_scores <- get_category_score(data, dfmat, fragments_dfm)

  # Hyper parameter combinations
  as                                   <- c(1.1, 1.5, 2)
  ns                                   <- c(0.1, 0.2, 0.3)
  bs                                   <- c(0.25, 0.5, 0.75)
  corp_normalizations_per_doc          <- c(TRUE, FALSE)
  category_normalizations_per_category <- c(TRUE, FALSE)
  use_index_scores                     <- c(TRUE, FALSE)

  # BEST VALUES:
  # a = 1.5
  # n = .1
  # b = .75
  # corp_normalizations_per_doc = FALSE
  # category_normalizations_per_category = FALSE
  # use_index_scores = FALSE

  batch_count <- length(as) *
    length(ns) *
    length(bs) *
    length(corp_normalizations_per_doc) *
    length(category_normalizations_per_category) *
    length(use_index_scores)
  i           <- 1
  start_eta()
  for(a in as) {
    for(n in ns) {
      for(b in bs) {
        for(normalize_corp_per_doc in corp_normalizations_per_doc) {
          for(normalize_cat_per_cat in category_normalizations_per_category) {
            for(use_index_score in use_index_scores) {

              # Compute corpus score, join on features
              features_current <- features
              corpus_scores    <- get_corpus_score(dfmat, a, n)
              scores           <- left_join(category_scores, corpus_scores, by = "term")

              # Add Category and Corpus scores
              features_current %<>% left_join(scores, by = c("lemma" = "term", "category" = "category"))

              # Nan to 0, else normalisation doesn't work
              features_current$corpus_score[is.na(features_current$corpus_score)]     <- 0
              features_current$category_score[is.na(features_current$category_score)] <- 0

              # Remove all words of which the lemma occurs <=2 times in the complete corpus. These are so rare that they should not be taken into account
              features_current %<>% group_by(lemma) %>% filter(n() > 2)

              # Corpus and Category score normalization
              if(normalize_corp_per_doc) {
                features_current %<>% group_by(doc_id) %>% mutate(corpus_score = (corpus_score - min(corpus_score)) / (max(corpus_score) - min(corpus_score)))
              }else {
                features_current$corpus_score %<>% normalize()
              }
              if(normalize_cat_per_cat) {
                features_current %<>% group_by(category) %>% group_modify(function(group, .) {
                  group$category_score %<>% log() %>% normalize(TRUE)
                  return(group)
                })
              }else {
                features_current$category_score %<>% log() %>% normalize(TRUE)
              }

              features_current$term_score <- ((b * features_current$category_score) + ((1 - b) * features_current$corpus_score)) / 2

              # Equation 12
              if(use_index_score) {
                features_current$final_score <- features_current$term_score *
                  features_current$pos_score *
                  features_current$index_score
              }else {
                features_current$final_score <- features_current$term_score * features_current$pos_score
              }

              # Nan to 0
              features_current$final_score[is.na(features_current$final_score)] <- 0

              result_loose  <- compute_f1(features_current, testset_loose)
              result_strict <- compute_f1(features_current, testset_strict)
              result_loose %<>% append(list(
                "a"                      = a,
                "n0"                     = n,
                "b"                      = b,
                "normalize_corp_per_doc" = normalize_corp_per_doc,
                "normalize_cate_per_cat" = normalize_cat_per_cat,
                "use_index_score"        = use_index_score
              ))
              result_strict %<>% append(list(
                "a"                      = a,
                "n0"                     = n,
                "b"                      = b,
                "normalize_corp_per_doc" = normalize_corp_per_doc,
                "normalize_cate_per_cat" = normalize_cat_per_cat,
                "use_index_score"        = use_index_score
              ))

              results_loose %<>% rbind(result_loose)
              results_strict %<>% rbind(result_strict)
              eta(i, batch_count, "%s per configuration, done at %s")
              i <- i + 1
            }
          }
        }
      }
    }
  }
  results_loose %<>% as.data.frame()
  results_strict %<>% as.data.frame()
  return(list(results_loose, results_strict))
}

# ==================================================================================================================== #
# ANNOTATION FUNCTIONS: CREATING AND PARSING DATA FOR DOCCANO
# ==================================================================================================================== #

#' Create a JsonL file for doccano import
#' @param data: The original data
#' @param n: The number of reviews to select. Also used for seeding, for reproduction
create_dataset_for_doccano <- function(data, n = 300) {
  # Reproduction by value of n
  set.seed(n)
  selection <- data %>% sample_n(n)
  result    <- c()
  for(i in 1:nrow(selection)) {
    result %<>% c(list("text" = selection[[i, "text"]], "review_id" = selection[[i, "review_id"]]) %>% toJSON())
  }
  f <- file(sprintf("../data/doccano_%s.jsonl", n))
  writeLines(result, f)
  close(f)
}

#' Parse doccano annotations to a dataframe
#' @param dir: The directory as downloaded and unzipped from doccano
#' @param features: The features matrix, one row for each token in the corpus
#' @return: Frame that contains the annotations. One row for each selected keyword. Columns:
#' - review_id,
#' - user: the user that made the annotation
#' - annotation: ID of the annoation, unique for each user-review
#' - feature_id: The id in 'features' to which the annotation is mapped
#' - word_by_substring: The word as extracted from the review text by the keyword boundaries
#' - lemma_by_features: The lemma of the feature identified by feature_id
#' - word_by_features: The (unprocessed) token of the feature identified by feature_id
read_doccano_files <- function(dir, features) {
  files <- list.files(path = dir, pattern = "*.jsonl", full.names = TRUE, recursive = TRUE)
  # Loop over the files, extract the user and the annotations. Return as list
  data  <- map(files, function(f) {
    user   <- gsub(".*(?:/)(.*).jsonl", "\\1", f)
    result <- readLines(f) %>% lapply(fromJSON)
    result %<>% lapply(function(a) {
      a['user'] <- user
      return(a)
    })
    return(result)
  }) %>% flatten()
  # List to frame
  frame <- rbindlist(data, fill = TRUE)
  # Remove the 'unknown' annotation
  frame %<>% filter(user != 'unknown')

  # Add column 'annotation': Unique ID for each review-user combination
  frame$annotation <- frame %>% group_indices(review_id, user)

  # Set names on column 'labels', such that 'unnest_wider' will split into right column names
  frame$label %<>% lapply(function(x) {
    tryCatch({ setNames(x, c("start", "end", "type")) }, error = function(e) { return(list()) }) %>% return()
  }

  ) # Set indices of label column
  frame %<>% unnest_wider(label) # Split label="(start, end, type)", into three columns
  frame <- frame[!is.na(frame$type),]

  frame$word_by_substring <- # Get the words from the texts by the indices, also clean
    substr(frame$data, frame$start, frame$end) %>%
      str_trim() %>% # Surrounding spaces
    { gsub("[^[:alnum:] ]", "", .) } # Non alphanum chars

  # Add columns feature_id, word_by_features, and token_by_features to table
  # Select those rows of which we have annotations
  features <- features[features$doc_id %in% unique(frame$review_id),]
  to_join  <- tibble("feature_id" = features$feature_id, "review_id" = features$doc_id, "idx" = features$idx, "lemma_by_features" = features$lemma, "word_by_features" = features$token)
  frame %<>% left_join(to_join, by = c("review_id" = "review_id", "start" = "idx"))

  # Each lemma once for each annotation. If a lemma is selected more than once for a review by a user, select only one
  frame %<>%
    group_by(lemma_by_features, annotation) %>%
    slice(1)

  return(frame)
}

#' Compute the test set out of the annotations:
#' - For each review that is annotated, select the keywords according to the annotators
#' - A word is a keyword is it is selected by at least min_annotations_for_selection annotators as a keyword
#' - min_annotations_for_selection is max(2, num_annotators/2)
#' - Return a tibble with one column: keyword. The values are the lemmas of the selected keywords
#' @param annotations: The annotations, one rows for each selected keyword
#' @param loose: If true, min_annotations_for_selection = 1. Eg every keyword that is selected, will be in the resulting testset
#' @return: Tibble
compute_testset <- function(annotations, loose = FALSE) {
  keywords <- annotations %>%
    group_by(review_id) %>%
    group_modify(function(annotations_for_review, .) {
      # The number of annotators that annotated this review
      num_annotations <- n_distinct(annotations_for_review$annotation)
      if(loose) {
        min_annotations_for_selection <- 1
      }else {
        min_annotations_for_selection <- max(ceiling(num_annotations / 2), 2)
      }
      # The keywords are those keywords that are selected by min_annotations_for_selection users
      keywords <- annotations_for_review %>%
        group_by(lemma_by_features) %>%
        tally() %>%
        filter(n >= min_annotations_for_selection) %>% # If marked by at least min_annotations_for_selection
      { .$lemma_by_features } %>% # Convert to unique list of feature ids
        unique()
      return(tibble("keyword" = keywords))
    })
  return(keywords)
}

#' Compute Inter Annotator Agreement:
#' - For each review that was annotated
#' - The 'gold_standard', eg the words that are keywords, are those words that were selected as keyword for this review,
#'      by at least max(2, num_reviewers / 2) reviewers. num_reviewers is the number of annotators that annotated this review
#' - For each user that annotated this review:
#' - Compute precision, recall, f1.
#' - Return tibble. One row for each user-review combination, containing the scores
#' @param annotations: The annotations, one row for each keyword
#' @return: The tibble with scores
compute_iaa <- function(annotations) {
  # We use F1
  b      <- 1
  result <- annotations %>%
    group_by(review_id) %>%
    group_modify(function(annotations_for_review, review_id) { # For each review
      # num_annotations is the number of annotators that annotated this review
      num_annotations                   <- n_distinct(annotations_for_review$annotation)
      min_annotations_for_gold_standard <- max(ceiling(num_annotations / 2), 2)
      # min_annotations_for_gold_standard <- 2
      # The gold standard are the keywords that are selected by min_annotations_for_gold_standard users
      gold_standard                     <- annotations_for_review %>%
        group_by(lemma_by_features) %>%
        tally() %>% # Count number of annotations for each lemma
        filter(n >= min_annotations_for_gold_standard) %>% # If marked by at least min_annotations_for_gold_standard
      { .$lemma_by_features } %>% # Convert to unique list of lemmas
        unique()
      scores                            <- annotations_for_review %>%
        group_by(annotation) %>%
        group_map(function(annotation, annotation_id) { # For each user that annotated this review
          user <- annotation[[1, 'user']] %>% as.character()

          # List of the words marked as keyword
          positive <- unique(annotation$lemma_by_features)

          # Count of words that are in the gold standard and in the positive list
          true_positive_count <- length(intersect(gold_standard, positive))

          precision <- true_positive_count / length(positive)
          recall    <- true_positive_count / length(gold_standard)
          f1        <- (1 + b^2) * (precision * recall) / (b^2 * (precision + recall))
          return(list(
            "user"      = user,
            "precision" = precision,
            "recall"    = recall,
            "f1"        = f1
          ))
        })

      # To frame
      frame <- rbindlist(scores)
      return(frame)
    })
  # Nan to 0
  result %<>% replace(is.na(result), 0)
  # Print and return results
  print("Inter annotator agreement:")
  print(sprintf("Mean F1: %#.3f", mean(result$f1)))
  print(sprintf("Mean Precision: %#.3f", mean(result$precision)))
  print(sprintf("Mean Recall: %#.3f", mean(result$recall)))
  return(result)
}

#' Parse doccano annotations:
#' - Read the files from filesystem, create a tabble with all annotations, one row for each keyword
#' - Compute the IAA
#' - Compute the testset
#' - return (iaa, testset)
#' @param dir: The directory containing all doccano files
#' @param features: The features matrix, one row for each word in the corpus
#' #' @param loose: Used when computing the testset. If true, min_annotations_for_selection = 1. Eg every keyword that is selected, will be in the resulting testset, instead of
#'      the strict requirement of max(2, num_annotations / 2)
parse_doccano <- function(dir, features, loose = FALSE) {
  annotations <- read_doccano_files(dir, features)
  # Filter out any mismatches. A mismatch is an annotation where word_by_features doesnt match word_by_substring, or feature_id is not found
  annotations %<>% filter(word_by_features == word_by_substring & !is.na(feature_id))
  # Extract the ids that were at least twice annotated, will use only those
  at_least_twice_annotated <- annotations %>%
    group_by(review_id) %>%
    summarize(n = length(unique(annotation))) %>%
    filter(n > 1) %>%
  { .$review_id }
  # Drop others
  annotations %<>% filter(review_id %in% at_least_twice_annotated)

  iaa     <- compute_iaa(annotations)
  testset <- compute_testset(annotations, loose)
  return(list(iaa, testset))
}

# ==================================================================================================================== #
# LOADING THE DATA
# ==================================================================================================================== #

#' Get the DFM on fragment level. EG each document is a fragment. Fragments are split using
#' - Punctuation
#' - Coordination conjunction https://universaldependencies.org/docs/u/pos/CONJ.html (POS tag is CCONJ, instead of CONJ)
#' The DFM thus contains one row per fragment, and the document variables of the review are also added
#' @param data: The raw data, one row per review
#' @param features: Features matrix, one row per term in each doc
#' @return: The DFM
get_fragments_dfm <- function(data, features) {
  # Compute break locations
  fragment_breaks  <- rownames(features[features$pos == "PUNCT" | features$pos == "CCONJ",]) %>%
    as.numeric() %>%
    prepend(0)
  # We map 'token id' to 'fragment id'. The breaks are mapped to 'NULL'
  word_to_fragment <- list()
  for(i in 1:(length(fragment_breaks) - 1)) {
    word_to_fragment[(fragment_breaks[i] + 1):(fragment_breaks[i + 1] - 1)] <- i
  }
  # Very last token is skipped, assign to same fragment as current last. And convert to chars
  word_to_fragment <- append(word_to_fragment, word_to_fragment[length(word_to_fragment)]) %>% as.character()

  # Create a mapping from fragment ids to review ids
  fragment_to_review <-
    tibble('doc_id' = features$doc_id, 'fragment_id' = word_to_fragment) %>% # Tibble with two columns
    { .[.$fragment_id != "NULL",] } %>% # Drop NULL fragments (the breaks)
    { setNames(as.integer(as.character(.$doc_id)), .$fragment_id) } %>% # Convert to named list
    { .[unique(names(.))] } # One row per fragment

  # Create the fragments features matrix
  fragments_features        <- features
  # Set fragment ids as document ids, instead of review ids. Now get_dfm will create one doc per fragment
  fragments_features$doc_id <- word_to_fragment
  # Remove the breakpoint words
  fragments_features        <- fragments_features[fragments_features$doc_id != "NULL",]

  fragments_dfm                       <- get_dfm(fragments_features)
  # Set review_id docvar
  docvars(fragments_dfm, 'review_id') <- fragment_to_review[docnames(fragments_dfm)]
  # Add other docvars
  fragments_dfm %<>% add_docvars(data)
  return(fragments_dfm)
}

#' Get the DFM on review level level. EG each document is a review.
#' The DFM thus contains one row per review, and the document variables of the review are also added
#' @param features: Features matrix, one row per term in each doc
#' @param data: The raw data, eg one row per review
#' @return: The DFM
get_reviews_dfm <- function(data, features) {
  dfmat                       <- get_dfm(features)
  # Set review_id docvar: The doc id is the review id
  docvars(dfmat, 'review_id') <- docid(dfmat) %>% as.character() %>% as.integer()

  # Add other docvars
  dfmat %<>% add_docvars(data)
  return(dfmat)
}

#' Get a DFM. A DFM contains one row per document, and one column per unique word in complete
#' collection of documents. The cell value defines how often the word occurs in the doc (BOW).
#' @param features: Features matrix, one row per term in each doc
#' @return DFM: One row for each document, one column for each unique word in the corpus
get_dfm <- function(features) {
  # Remove punctuation, spaces, and stopwords
  features %<>% { .[.$pos %in% c('PUNCT', 'SPACE') %>% `!` & !.$is_stop,] }
  dfmat <- features %>%
    as.tokens(use_lemma = TRUE) %>% # features data to tokens, use lemma
    tokens_remove(stopwords("nl"), case_insensitive = TRUE, verbose = TRUE) %>% #Remove stopwords
    dfm() #To Document-Frequency-Matrix
  return(dfmat)
}

#' Add docvars to a dfm. Assumes that the docvar 'review_id' yet exists; it is used for indexing 'data'
#' @param dfmat: The dfm to add docvars to
#' @param data: The original data, eg one row per review
#' @return: The dfm, with docvars added
add_docvars <- function(dfmat, data) {
  review_ids <- docvars(dfmat, 'review_id')
  for(var in list("provider", "grade", "url", "type", "date", "condition", "department", "text", "grade_class", "original_review_id")) {
    docvars(dfmat, var) <- data[review_ids, var]
  }
  return(dfmat)
}

#' Get features: Eg matrix with one row for each word in each document.
#' Matrix is created using spacy, and contains:
#' - doc_id
#' - token
#' - lemma
#' - idx (pos in doc in chars)
#' - lower_ (word to lower)
#' - is_stop: Whether the words is a stopword
#' - sentiment: Sentiment score of the word (not used for now)
#' - pos_score: POS weight as in 3.3.3 of the paper, using pos weights of pos_weights()
get_features <- function(data) {
  # Generate features (this takes time)
  spacy_initialize(model = "nl_core_news_sm")
  features        <- spacy_parse(data$text, dependency = TRUE, additional_attributes = c('idx', 'lower_', 'is_stop', 'sentiment'))
  # Set doc id equal to review id
  features$doc_id <- data[as.integer(str_remove(features$doc_id, "text")),]$review_id

  # Set pos weights
  features$pos_score                            <- pos_weights()[features$pos] %>%
    as.character() %>%
    as.numeric()
  # Different from the paper. If the POS-tag has no weight, assign score of .1 instead of 0. If spacy incorrectly parsed
  # the word, we allow for the possibility that the word is still relevant, although minor
  features$pos_score[is.na(features$pos_score)] <- .1

  # Add category, grade, grade_class
  extra_data <- tibble('doc_id' = data$review_id, "provider" = data$provider, 'grade' = data$grade, 'grade_class' = (data$grade >= 7) %>% as.integer())
  features %<>% left_join(extra_data, by = "doc_id")

  # Add unique feature id
  features$feature_id <- seq.int(nrow(features))

  return(features)
}

# ==================================================================================================================== #
# COMPUTING SCORES
# As in Section 3.3 of the paper
# ==================================================================================================================== #

#' The corpus level score is the IDF_fw for each term, and the first part of the final score
#' Described in Section 3.3.1 of the paper
#' @param dfmat: The dfm
#' @param a: Alpha parameter. Best value according to evaluate_hyperparameters is 1.5
#' @param n: n_0 parameter: percentage of docs for FW. Best value according to evaluate_hyperparameters is 0.1
#' @return: Tibble with columns 'feature', 'corpus_score'
get_corpus_score <- function(dfmat, a = 1.5, n = 0.1) {
  n_docs       <- dim(dfmat)[[1]]
  tf_c         <- colSums(dfmat)
  idf          <- colSums(dfmat > 0) %>% { log2(n_docs / .) }
  n_0          <- n * n_docs
  # Equation 3
  fw           <- a * abs(log2(tf_c / n_0))
  # Equation 2
  idf_fw       <- idf - fw
  corpus_score <- tibble("term" = names(idf_fw), "corpus_score" = idf_fw)
  return(corpus_score)
}


#' Get the Term-Corpus relevance for each feature. This is the first part of the category_score
#' - For each feature compute the average fragment length and the category count
#' - Compute TCoR. Described in Section 3.3.2 of the paper.
#' @param fragments_dfm: The DFM on fragment level, eg one document per fragment.
#'      Should contain a column 'category' that defines the category of a review. This column value is set on data using set_category_column_on_data, which is copied to 'fragments_dfm' in get_category_score
#' @return: Tibble with columns 'feature', 'TCoR'
get_tcor <- function(fragments_dfm) {
  # DFM to tidy: One row for each unique fragment-word. Cols document, term, count
  frame <- dfm.tidy(fragments_dfm)

  # Add fragment_length col
  fragment_lengths      <- rowSums(fragments_dfm)
  frame$fragment_length <- fragment_lengths[frame$document]

  # Add category col
  categories     <- setNames(docvars(fragments_dfm, "category"), docnames(fragments_dfm))
  frame$category <- categories[frame$document]

  # Compute mean fragment length and category count
  scores <- frame %>%
    group_by(term) %>% # Group on term: group per unique word in corpus
    summarize("mean_fragment_length" = mean(fragment_length),                # Mean of the fragments lenghts of the docs
      "category_count"               = n_distinct(category)                  # Number of unique categories for each word
    )

  # Compute category score for each word. Equation 4
  scores$tcor <- (1 / scores$mean_fragment_length) + (1 / scores$category_count)

  return(scores[, c('term', 'tcor')])
}

#' Compute the TCaR: the second part of the category score
#' - Compute the number of documents per category and the number of documents per term (denominators)
#' - Compute the number of documents with term t of type c for each type-category combination (nominator)
#' - Compute p(c|t) and p(t|c) by dividing the nominator by both denominators
#' - Compute the TCaR by summing both probabilities. Described in Section 3.3.2 of the paper.
#' @param data: The raw data, eg one row for each review.
#'     Should contain a column 'category' that defines the category of a review.This column value is set on data using set_category_column_on_data, which is copied to 'fragments_dfm' in get_category_score
#' @param dfmat: The DFM of the data
#' @return: Tibble with columns 'feature', 'category', 'TCaR', 'TCoR', 'category_score'
get_tcar <- function(data, dfmat) {
  # 2 columns: category, n_docs_of_c
  n_docs_per_category <- data %>%
    group_by(category) %>%
    tally(name = "n_docs_of_c")

  # 2 columns: term, num_docs_with_t
  num_docs_per_term <- colSums(dfmat > 0) %>% { tibble("term" = names(.), "num_docs_with_t" = .) }

  # DFM to tidy: One row for each unique fragment-word. Cols document, term, count
  frame <- dfm.tidy(dfmat)

  # Add category col
  categories     <- setNames(docvars(dfmat, 'category'), docnames(dfmat))
  frame$category <- categories[frame$document]


  # Create a 3-column table: category, term, num docs of type category containing term t
  # One row for each unique category-term combination
  num_docs_of_c_with_t <- frame %>%
    group_by(category, term) %>% # One group for each unique term in each category
    # Each group has one row per doc, as 'frame' had one row per doc-feature. Thus tally counts the number of docs
    tally(name = "num_docs_of_c_with_t") # Below line
  # Add num_docs_per_term and num_docs_per_category
  scores               <- left_join(num_docs_of_c_with_t, num_docs_per_term, by = "term") %>% left_join(n_docs_per_category, by = "category")
  # Compute P(c|t) by (num docs of type c containing t) / (total num docs containing t). Equation 5
  scores$p_c_given_t   <- scores$num_docs_of_c_with_t / scores$num_docs_with_t
  # Compute p(t|c) by (num docs of type c containing t) / (total num docs of type c). Equation 6
  scores$p_t_given_c   <- scores$num_docs_of_c_with_t / scores$n_docs_of_c
  # Compute TCaR by p(c|t) + p(t|c). Equation 7
  scores$tcar          <- scores$p_c_given_t + scores$p_t_given_c
  return(scores[, c('term', 'category', 'tcar')])
}

#' Get the category score for each feature, computed as in section 3.3.2 of the paper. It is the second part of the
#' final score:
#' - Get the TCoR
#' - Get the TCaR
#' - Compute category score by TCoR * TCaR
#' @param data: The cleaned data, eg tibble where each row is a review
#' @param dfmat: The DFM
#' @param fragment_dfm:  The DFM for the fragments
#' @return: Tibble with columns 'feature', 'category_score'
get_category_score <- function(data, dfmat, fragment_dfm) {
  # First add column 'category' to dfmat and fragments_dfm
  docvars(dfmat, 'category')        <- data[docvars(dfmat, 'review_id'), 'category']
  docvars(fragment_dfm, 'category') <- data[docvars(fragment_dfm, 'review_id'), 'category']


  tcor <- get_tcor(fragment_dfm)
  write_log('TCOR computed')
  tcar <- get_tcar(data, dfmat)
  write_log('TCAR computed')
  result                <- left_join(tcar, tcor, by = "term")
  # Equation 8
  result$category_score <- result$tcor * result$tcar
  return(result)
}

#' Add the term score to the featues matrix. Section 3.3.3 of the paper
#' - First drop the PUNCT and SPACE features. They are not relevant, and should be excluded in the counts
#' - Add column index_score, based on the doc_length
#' - Add column term_score, which is the combination of category and corpus score
#' - Add column final_score, that multiplies the scores via Equation 12
#' @param features: The original feature matrix: One row per word in each doc
#' @param data: The original data, one row per review. A column 'category' should have been added using set_category_column_on_data
#' @param dfmat: The Document-Frequency-Matrix: One row per review, one col per unique word in the corpus
#' @param fragments_dfm: The Document-Frequency-Matrix: One row per fragment, one col per unique word in the corpus
#' @return: The scores: Tibble with one row for each unique word in each category, has column 'score'
add_scores <- function(features, data, dfmat, fragments_dfm) {
  # Add column 'category' on features
  features$category <- data[features$doc_id,]$category

  # Drop PUNCT, SPACE and stopwords
  features %<>% { .[.$pos %in% c('PUNCT', 'SPACE') %>% `!` & !.$is_stop,] }
  # Equation 11: Add index_score via doc_length and feature_index - d(t) as in 3.3.3 of the paper.
  features %<>% group_by(doc_id) %>% add_tally(name = "doc_length")
  features %<>% group_by(doc_id) %>% mutate(feature_index = seq.int(n()) - 1)
  features$index_score <- 1 - features$feature_index / features$doc_length

  # Compute scures
  corpus_score <- get_corpus_score(dfmat)
  write_log("Corpus score computed")
  category_score <- get_category_score(data, dfmat, fragments_dfm)

  scores <- left_join(category_score, corpus_score, by = "term")
  write_log("Category score computed")

  # Add Category and Corpus scores
  features %<>% left_join(scores, by = c("lemma" = "term", "category" = "category"))

  # Nan to 0, else normalisation doesn't work
  features$corpus_score[is.na(features$corpus_score)]     <- 0
  features$category_score[is.na(features$category_score)] <- 0

  # Remove all words of which the lemma occurs <=2 times in the complete corpus. These are so rare that they should not be taken into account
  features %<>% group_by(lemma) %>% filter(n() > 2)

  # Normalize category score to [0, 1]. Not done in paper. But makes category scores comparable with corpus scores
  # Best hyperparemeter value: normalize over whole corpus
  features$category_score_raw <- features$category_score
  # Not done in paper: Also apply log transform. This improves the scale of the category score wrt corpus score greatly
  features$category_score %<>% log() %>% normalize(TRUE)

  # Equation 9: normalize Corpus scores
  # Best hyperparameter  value: normalize over the whole corpus
  features$corpus_score_raw <- features$corpus_score
  features$corpus_score %<>% normalize()

  write_log("Scores normalized")

  # Equation 10: Compute term score by corpus and category score.
  b                   <- .5
  features$term_score <- ((b * features$category_score) + ((1 - b) * features$corpus_score)) / 2

  # Equation 12
  # Best hyperparameter value: do not use index_score
  features$final_score <- features$term_score * features$pos_score

  # Nan to 0
  features$final_score[is.na(features$final_score)] <- 0

  write_log("Final scores computed")

  return(features)
}

#' Drop double lemmas within each category. Keep the lemma with the highest final_score.
#' Also add column 'freq', which defines the frequency of the lemma within the category
#' @param scores: The scores frame
#' @param category_column: The column that defines the category. Eg in the resulting frame each lemma occurs at most once for each category
#' @return: scores, with double lemmas dropped and 'freq' column added
drop_double_lemmas <- function(scores, category_column) {
  scores %<>% group_by(.data[[category_column]], lemma) %>%
    add_tally(name = 'freq') %>%
    slice_max(final_score, n = 1, with_ties = FALSE)
  return(scores)
}

#' Get the keywords for the features
#' The features contains one row for each document-term
#' The keywords extract only those terms with a high final_score:
#' At most 10 keywords per doc, with at least a score of 0.5 * the highest score for that doc
#' @param scores: The frame with the scores. Tidy: One row for each document-term
#' @param category_column: The column that defines the category. For each category, we take the top 10 keywords
#' @param drop_doubles: If true, we drop all double lemmas within each category, before extracting keywords
#' @return: The scores. Same structure, but only the high scoring words remain.
get_keywords <- function(scores, category_column = 'doc_id', drop_doubles = TRUE) {
  if(drop_doubles) {
    scores %<>% drop_double_lemmas(category_column)
  }
  # Take only those keywords with at least .5 * max, max 10
  keywords <- scores %>%
    group_by(.data[[category_column]]) %>%
    filter(final_score >= 0.5 * max(final_score)) %>%
    slice_max(final_score, n = 10, with_ties = FALSE)
  return(keywords)
}

#' Retrieve the keywords for a doc, and print them
#' @param data: The original data, one row per review
#' @param scores: The scores matrix, one row for each unique word in each category
#' @param doc_id: The id of the doc to sample
sample_keywords <- function(features, data, doc_id) {
  doc    <- features[features$doc_id == doc_id,]
  output <- tibble("Word" = doc$lemma, "Cat score" = doc$category_score, "Corp score" = doc$corpus_score, "Score" = doc$final_score)
  text   <- data[doc_id,]$text
  writeLines(sprintf("(%s) Keywords for text\n%s: ", doc_id, text))
  print(output)
}

# ===================================
# RUN:
# - Load the complete dataset from reviews.RDS
# - Uncomment lines below to extract keywords
# ===================================

#' Initialize, by setting global vars, and loading data from FS. If data is not found in FS, will be created.
#'
#' Note:
#' Performing any operations on low aggregation level (review_id) is very slow, because data %>% group_by(level, word)
#' is slow with many groups.
#'
keyword_extraction.init <- function() {
  # Can't run if scraper wasn't ran
  if(!file.exists('../data/reviews.RDS')) {
    write_log("Please run the scraper before the vizualizer. Save the scraped data as ../data/reviews.RDS")
    # q()
  }

  # Load data from FS
  reviews <<- readRDS('../data/reviews.RDS')
  assign("data", reviews, envir = .GlobalEnv)
  data$grade_class        <<- (data$grade >= 7) %>% as.integer()
  data$grade_int          <<- data$grade %>% as.integer()
  data$original_review_id <<- data$review_id
}

keyword_extraction.init()

# Subset data (optional)
# data <- data[data$provider == 'PROVIDER',]
# If using subset, make sure review ids are sequential
# data$original_review_id <- data$review_id
# data$review_id <- seq.int(nrow(data))

# Get features
features <- get_features(data)
# saveRDS(features, '../data/features_raw.RDS')
write_log('features computed and saved saved')

# Features to DFM
dfmat         <- get_reviews_dfm(data, features)
fragments_dfm <- get_fragments_dfm(data, features)
# saveRDS(dfmat, '../data/dfmat.RDS')
# saveRDS(fragments_dfm, '../data/fragments_dfm.RDS')
write_log('DFMs created and saved')

# Compute scores. First set the category column on data
data %<>% set_category_column_on_data(c('provider'))
scores <- features %>% add_scores(data, dfmat, fragments_dfm)
# saveRDS(scores, '../data/scores.RDS')
write_log('Scores computed and saved')

# Get the keywords: At most 10 per doc, at least .5 of the highest word in each doc
keywords <- get_keywords(scores)
write_log('Keywords computed')

# Sample result: Select one doc, print text and keywords
# review_id <- unique(keywords$doc_id) %>% sample(1)
# sample_keywords(keywords, data, review_id)

# Parameter tuning
# evaluation_result <- evaluate_hyperparameters(features, data, dfmat, fragments_dfm)
# evaluation_loose <- evaluation_result[[1]]
# evaluation_strict <- evaluation_result[[2]]
# saveRDS(evaluation_loose, '../data/evaluation_loose.RDS')
# saveRDS(evaluation_strict, '../data/evaluation_strict.RDS')

# POS-weight tuning
# pos_evaluation_result <- evaluate_pos_weights(features, data, dfmat, fragments_dfm)
# pos_evaluation_loose <- pos_evaluation_result[[1]]
# pos_evaluation_strict <- pos_evaluation_result[[2]]
# saveRDS(pos_evaluation_loose, '../data/pos_evaluation_loose.RDS')
# saveRDS(pos_evaluation_strict, '../data/pos_evaluation_strict.RDS')

# Doccano functionality
# create_dataset_for_doccano(data, 300)
# doccano_results <- parse_doccano('../data/doccano', features)
# iaa <- doccano_results[[1]]
# testset <- doccano_results[[2]]
# res <- compute_f1(scores_default, testset)

# Other
# gc()