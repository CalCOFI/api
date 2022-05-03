# CalCOFI API endpoints

# packages ----
if (!require("librarian")){
  install.packages("librarian")
  library(librarian)
}
librarian::shelf(
  DBI, dbplyr, dplyr, glue, here, lubridate, RPostgres, sf, stringr, tidyr)

# database ----
db_pass_txt <- "~/.calcofi_db_pass.txt"
stopifnot(file.exists(db_pass_txt))

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = "gis",
  host     = "db.calcofi.io",
  port     = 5432,
  user     = "admin",
  password = readLines(db_pass_txt))

# helper functions ----
glue2 <- function(x, null_str="", .envir = sys.frame(-3), ...){
  # handle NULLs inside glue string as empty character
  null_transformer <- function(str = "NULL") {
    function(text, envir) {
      out <- glue::identity_transformer(text, envir)
      if (is.null(out))
        return(str)
      out }}
  glue(x, .transformer = null_transformer(null_str), .envir = .envir, ...)
}

# /variables ----
#* Get list of variables
#* @get /variables
#* @serializer csv
function() {
  # TODO: show summary stats per variable:
  #   date_beg, date_end, depth_min, depth_max, n_records
  
  tbl(con, "field_labels") %>% 
    # filter(table %>% starts_with("cast_bottle")) %>% 
    collect()
}

# /timeseries ----
#* Get time series data
#* @param variable The variable of interest. Must be one listed in `/variables`.
#* @param aoi_wkt Area of Interest (AOI) spatially described as well known text (WKT). Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_min Depth (meters) minimum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_max Depth (meters) maximum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_beg Date to begin, e.g. "2000-01-01". Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_end Date to end, e.g. "2020-12-31". Defaults to NULL, i.e. no filter or entire dataset.
#* @param time_step time step over which to summarize; one of: a sequential increment ("decade", "year", "year.quarter", "year.month", "year.week", "date") or a climatology ("quarter","month","week","julianday","hour"). default is "year". If NULL then all values are returned, i.e. no aggregation by `time_step` or `stats` are applicable.
#* @param stats Statistics to show per `time_step`. Defaults to "mean, p05, p95". Acceptable comma-separated values include: "min", "median", "max", "sd" or "p#" where "sd" is the standard deviation and "p#" represents the percentile value 0 to 100 within available range of values for given aggregated `time_step`.
#* @get /timeseries
#* @serializer csv
function(
  variable = "ctdcast_bottle.t_deg_c", 
  aoi_wkt = NULL, 
  depth_m_min = NULL, depth_m_max = NULL,
  date_beg = NULL, date_end = NULL, 
  time_step = "year",
  stats = "avg, q10, q90"){
  
  # DEBUG
  # variable = "ctdcast_bottle.t_deg_c"
  # aoi_sf <- st_read(con, "aoi_fed_sanctuaries") %>%
  #   filter(nms == "CINMS")
  # aoi_wkt <- aoi_sf %>%
  #   pull(geom) %>%
  #   st_as_text()
  # st_bbox(aoi_sf) %>% st_as_sfc() %>% st_as_text()
  # aoi_wkt <- "POLYGON ((-120.6421 33.36241, -118.9071 33.36241, -118.9071 34.20707, -120.6421 34.20707, -120.6421 33.36241))"
  # date_beg = NULL; date_end = NULL
  # time_step = "year"
  # stats = "mean, p10, p90"
  # depth_m_min = NULL; depth_m_max = NULL
  
  # TODO: 
  
  # check input arguments ----
  
  # variable
  v <- tbl(con, "field_labels") %>% 
    filter(table_field == !!variable) %>% 
    collect() %>% 
    separate(table_field, into=c("tbl", "fld"), sep="\\.", remove=F)
  stopifnot(nrow(v) == 1)

  # TODO: is_valid_date(), is_valid_aoi(): wkt, aoi_id
  # if (!is.null(aoi_wkt))
  #   aoi_sf <- st_as_sf(tibble(geom = aoi_wkt), wkt = "geom") %>% 
  #     st_set_crs(4326)  # mapview::mapview(aoi_sf)
  
  # TODO: document DATE_PART() options
  # https://www.postgresql.org/docs/13/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
  # is_valid_aoi(aoi)
  # is_valid_date(date_beg)
  # is_valid_date(date_end)
  
  stopifnot(time_step %in% c("decade","year","year.quarter","year.month","year.week","date","quarter","month","week","julianday","hour"))
  # TODO: Describe non- vs climatalogical vars: "quarter","month","week","julianday"
  q_time_step <- switch(
    time_step,
    decade       = "(DATE_PART('decade' , date) * 10) AS decade",
    # year         = "DATE_PART('year'   , date) AS year",
    year         = "DATE_PART('year'   , date)",
    year.quarter = "DATE_PART('year'   , date) + (DATE_PART('quarter', date) * 0.1)  AS year.quarter",
    year.month   = "DATE_PART('year'   , date) + (DATE_PART('month', date)   * 0.01) AS year.month",
    year.week    = "DATE_PART('year'   , date) + (DATE_PART('week', date)    * 0.01) AS year.week",
    date         = "date",
    quarter      = "DATE_PART('quarter', date) AS quarter",
    month        = "DATE_PART('month'  , date) AS month",
    week         = "DATE_PART('week'   , date) AS week",
    julianday    = "DATE_PART('doy'    , date) AS julianday",
    hour         = "DATE_PART('hour'   , datetime) AS hour")
  if (is.null(q_time_step))
    q_time_step = "datetime"
  
  q_from <- case_when(
    v$tbl == 'ctdcast_bottle'     ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt)",
    v$tbl == 'ctdcast_bottle_dic' ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt) JOIN ctdcast_bottle_dic USING (btl_cnt)")

  q_where_aoi = ifelse(
    !is.null(aoi_wkt),
    glue("ST_Intersects(ST_GeomFromText('{aoi_wkt}', 4326), ctdcast.geom)"),
    "TRUE")

  q_where_date = case_when(
    !is.null(date_beg) & !is.null(date_end) ~ glue2("date >= '{date_beg}' AND date <= '{date_end}'"),
     is.null(date_beg) & !is.null(date_end) ~ glue2("date <= '{date_end}'"),
    !is.null(date_beg) &  is.null(date_end) ~ glue2("date >= '{date_beg}'"),
    TRUE ~ "TRUE")
  
  q_where_depth = case_when(
    !is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m >= '{depth_m_min}' AND depth_m <= '{depth_m_max}'"),
     is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m <= '{depth_m_max}'"),
    !is.null(depth_m_min) &  is.null(depth_m_max) ~ glue2("depth_m >= '{depth_m_min}'"),
    TRUE ~ "TRUE")
  
  # TODO: get median, percentile ----
  # https://leafo.net/guides/postgresql-calculating-percentile.html
  # https://www.postgresql.org/docs/9.4/functions-aggregate.html
  
  q <- glue(
    "SELECT {q_time_step} AS {time_step}, AVG({v$fld}) AS {v$fld}_avg, STDDEV({v$fld}) AS {v$fld}_sd, COUNT(*) AS n_obs
    FROM {q_from}
    WHERE {q_where_aoi} AND {q_where_date} AND {q_where_depth}
    GROUP BY {q_time_step} 
    ORDER BY {time_step}")
  message(q)
  d <- dbGetQuery(con, q)

  # TODO: add attributes like Cristina's original function  
  # attr(d_aoi_summ, "labels")    <- eval(parse(text = glue("var_lookup$`{var}`")))
  # attr(d_aoi_summ, "time_step") <- time_step
  # attr(d_aoi_summ, "date_msg")  <- glue("This dataset was summarized by {time_step}.")
  # attr(d_aoi_summ, "aoi") <- ifelse(
  #   empty_data_for_var,
  #   glue("No data were found for {var} in this area of interest. Summaries were conducted across all existing data points."),
  #   glue("Data for {var} in selected area of interest")
  # )
  
  d
}

# / ----
#* redirect to the swagger interface 
#* @get /
#* @serializer html
function(req, res) {
  res$status <- 303 # redirect
  res$setHeader("Location", "./__swagger__/")
  "<html>
  <head>
    <meta http-equiv=\"Refresh\" content=\"0; url=./__swagger__/\" />
  </head>
  <body>
    <p>For documentation on this API, please visit <a href=\"http://api.ships4whales.org/__swagger__/\">http://api.ships4whales.org/__swagger__/</a>.</p>
  </body>
</html>"
}

# /OLD... ----

#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg="") {
  list(msg = paste0("The message is: '", msg, "'"))
}

#* Plot a histogram
#* @serializer png
#* @get /plot
function() {
  rand <- rnorm(100)
  hist(rand)
}

#* Return the sum of two numbers
#* @param a The first number to add
#* @param b The second number to add
#* @post /sum
function(a, b) {
  as.numeric(a) + as.numeric(b)
}
