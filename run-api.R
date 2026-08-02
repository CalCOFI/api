#!/usr/bin/env Rscript
library(plumber)

# Resolve plumber.R RELATIVE TO THIS SCRIPT rather than by absolute path. The
# hardcoded "/share/github/api/plumber.R" broke when the repo moved under
# CalCOFI/ in the July 2026 reorganization, and because the container restarts
# `unless-stopped` it crash-looped rather than failing visibly. Deriving the path
# from the script's own location cannot break that way again.
args      <- commandArgs(trailingOnly = FALSE)
this_file <- sub("^--file=", "", args[grep("^--file=", args)])
plumber_r <- file.path(dirname(normalizePath(this_file)), "plumber.R")
stopifnot("plumber.R not found beside run-api.R" = file.exists(plumber_r))
message("serving ", plumber_r)

# error_handler <- function(req, res, err){
#   res$status <- 500
#   list(error = err$message)
# }

pr(plumber_r) %>%
  # pr_set_error(error_handler) %>%
  pr_run(port=8888, debug=T, host="0.0.0.0")
