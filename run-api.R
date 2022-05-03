#!/usr/bin/env Rscript
library(plumber)

plumber_r <- "/share/github/api/plumber.R"

# error_handler <- function(req, res, err){
#   res$status <- 500
#   list(error = err$message)
# }

pr(plumber_r) %>%
  # pr_set_error(error_handler) %>%
  pr_run(port=8888, debug=T, host="0.0.0.0")
