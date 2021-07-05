# ==================================================================================================================== #
# FUNCTIONALITY:
# Provide helper functions used in other scripts.
# - write_log writes a message to stdout, including timestamp
# - start_eta and eta are used for timing long-running functions
# ==================================================================================================================== #


# Set when start_eta() is ran, used to compute etas during long-running processes
timer <- Sys.time()

#' Write a log to stdou
#' @param msg: The msg
write_log <- function(msg) {
  time <- format(Sys.time(), "%H:%M:%S")
  print(sprintf("[%s] - %s", time, msg))
}

#' Start timer for ETA estimation
start_eta <- function() {
  timer <<- Sys.time()
}


#' Calculdate ETA
#' @param num_done: Number of items done
#' @param num_total: Number of items to do in total
#' @param message: If provided, write log "sprintf(message, time per entity, eta)". Thus should have 2 %s'es
#' @return [eta, time_per_entity (s)]. Only if missing(message)
eta <- function(num_done, num_total, message) {
  time_elapsed    <- difftime(Sys.time(), timer, units = "secs")
  time_per_entity <- time_elapsed / num_done
  time_todo       <- floor((num_total - num_done) * time_per_entity)

  estimation      <- format(Sys.time() + time_todo, "%H:%M:%S")
  time_per_entity <- round(time_per_entity, 2)

  if(!missing(message)) {
    write_log(sprintf(paste("[%s/%s] -", message), num_done, num_total, time_per_entity, estimation))
  }
  else {
    return(list(estimation, time_per_entity))
  }
}