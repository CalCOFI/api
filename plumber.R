# CalCOFI API endpoints

# packages ----
if (!require("librarian")){
  install.packages("librarian")
  library(librarian)
}
librarian::shelf(
  DBI, dbplyr, digest, dplyr, glue, gstat, here, httr2, lubridate, 
  plumber, raster, RPostgres, sf, stringr, tidyr)
# librarian::shelf(
#   DBI, dbplyr, digest, dplyr, glue, here, lubridate, 
#   raster, RPostgres, sf, stringr, tidyr)
select = dplyr::select

# paths ----
dir_apps <- here("../apps")
dir_cache <- "/tmp"

# database ----
source(glue("{dir_apps}/libs/db.R"))

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

# /cruises ----
#* Get list of cruises with summary stats as CSV table for period between start and end dates
#* @param start_date:str starting date for search
#* @param end_date:str end date for search
#* @get /cruises
#* @serializer csv
function(  start_date = "1949-01-01",
           end_date = Sys.Date() |> format("%Y-%m-%d")) {
  
  # TODO: add ctd_casts indexes to db for: cruise_id, date, lon_dec, lat_dec

  y <- tbl(con, "ctd_casts") %>% 
    group_by(cruiseid) %>% 
    summarize(
      cruise_ymd = min(cruise, na.rm=T),
      date_beg = min(date, na.rm=T),
      date_end = max(date, na.rm=T),
      lon_min  = min(longitude, na.rm=T),
      lon_max  = max(longitude, na.rm=T),
      lat_min  = min(latitude, na.rm=T),
      lat_max  = max(latitude, na.rm=T),
      n_casts = n(),
      .groups = "drop") %>% 
    arrange(desc(cruiseid)) %>% 
    filter(
      date_beg >= as.Date(start_date),
      date_end <= as.Date(end_date)
    ) %>%
    collect()
  names(y) <- toupper(names(y))
  y
}

# /ichthyo_species ----
#* Get alphabetic list of the scientific names of larvae species in the database
#* @get /ichthyo_species
#* @serializer csv
function() {
  
  l <- tbl(con, "larvae_species") %>% 
    distinct(scientific_name, common_name) %>%
    collect() |> 
    select(common_name, scientific_name)

  s <- tbl(con, "egg_species") %>% 
    distinct(scientific_name, common_name) %>%
    collect()

  y <- bind_rows(l, s) |> 
    distinct() |> 
    arrange(scientific_name)
  
  names(y) <- toupper(names(y))
  y
}


# /icthyo_variables ----
#* Get list of variables from the icthyoplankton dataset.  Note that "catch_per_effort" is a derived variable not stored in the database.
#* @get /icthyo_variables
function() {
  # TODO: show summary stats per variable:
  #   date_beg, date_end, depth_min, depth_max, n_records
  
  y <- tbl(con, "net2cruise") %>% 
   left_join(
      tbl(con, "larvae_species"), 
      by="netid") %>%
    collect()
  y1 <- tbl(con, "egg_species") %>% collect()
  unique(c(colnames(y), colnames(y1), c('catch_per_effort')))
}

# /larvaedata ----
# https://rest.calcofi.io/net2cruise?netid=gt.18149&netid=lt.18155&select=netid,cruise_ymd,latitude,longitude
#* Get fish larvae  data
#* @param cruiseymd_min:int min cruise identifier, must be one of cruise_ymd in cruises
#* @param cruiseymd_max:int max cruise identifier, must be one of cruise_ymd in cruises
#* @param species:str comma-separated list of scientific names (case-sensitive), or 'all'
#* @param fields:[str] fields to include in output, must be in list of values returned by /icthyo_variables
#* @serializer csv
#* @get /larvaedata
function(
    cruiseymd_max = 202301,
    cruiseymd_min = 202001,
    species='all',
    fields = c("netid", "cruise_ymd", "line", "station", "latitude", "longitude", "orderocc", "townumber", "towtype", "netside",  "scientific_name", "catch_per_effort")) {
  
  # debug by setting a browser
  # browser()
  
  # method 1: direct database connection
  catch <- FALSE
  if("catch_per_effort" %in% fields){
    fields <- union(fields, c("larvaecount", "towtype", "volsampled", "shf", "propsorted")) |>
      setdiff("catch_per_effort")
    catch <- TRUE
  }
  y <- tbl(con, "net2cruise") |>
    left_join(
      tbl(con, "larvae_species"), 
      by="netid") |>
  filter(
      as.integer(cruise_ymd) >= cruiseymd_min,
      as.integer(cruise_ymd) <= cruiseymd_max) |>
    select(all_of(fields)) |># https://dplyr.tidyverse.org/articles/programming.html
    collect()
    if(species != 'all'){
      sp_list <- trimws(unlist(strsplit(species,",")))
      y <- y %>% filter(scientific_name %in% sp_list)
    }
    if(catch){
        y <- y %>% mutate(catch_per_effort=if_else(towtype=="MT", 100*larvaecount/volsampled, shf*larvaecount/propsorted), .before=larvaecount)
     } 
  
  
  # method 2: postgres intermediary API
  # url <- "https://rest.calcofi.io/net2cruise?netid=gt.18149&netid=lt.18155&select=netid,cruise_ymd,latitude,longitude"
  # req <- request(url)
  # y <- req_perform(req) |> 
  #   resp_body_json()
  names(y) <- toupper(names(y))
  y
}


# /variables ----
#* Get list of variables for use in `/timeseries`
#* @get /variables
#* @serializer csv
function() {
  # TODO: show summary stats per variable:
  #   date_beg, date_end, depth_min, depth_max, n_records
  
  tbl(con, "field_labels") %>% 
    # filter(table %>% starts_with("cast_bottle")) %>% 
    filter(active) %>% 
    select(-active) %>% 
    collect()
}

# /species_groups ----
#* Get list of species groups for use with variable `larvae_counts.count` in `/timeseries`
#* @get /species_groups
#* @serializer csv
function() {
  tbl(con, "species_groups") %>% 
    left_join(
      tbl(con, "species_codes"), 
      by="spccode")
    collect()
}

# /timeseries ----
#* Get time series data
#* @param variable:str The variable of interest. Must be one listed in `/variables`.
#* @param species_group:str If using variable `larvae_counts.count`, then species grouping. Must be one listed in `/species_groups`.
#* @param aoi_wkt:str Area of Interest (AOI) spatially described as well known text (WKT). Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_min:int Depth (meters) minimum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_max:int Depth (meters) maximum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_beg:str Date to begin, e.g. "2000-01-01". Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_end:str Date to end, e.g. "2020-12-31". Defaults to NULL, i.e. no filter or entire dataset.
#* @param time_step:str time step over which to summarize; one of: a sequential increment ("decade", "year", "year.quarter", "year.month", "year.week", "date") or a climatology ("quarter","month","week","julianday","hour"). default is "year". If NULL then all values are returned, i.e. no aggregation by `time_step` or `stats` are applicable.
#* @param stats:[str] Statistics to show per `time_step`. Defaults to "p05", "avg", "p95". Acceptable comma-separated values include: "avg", "median", "min", "max", "sd" or "p#" where "sd" is the standard deviation and "p#" represents the percentile value 0 to 100 within available range of values for given aggregated `time_step`.
#* @get /timeseries
#* @serializer csv
function(
  variable      = "ctd_bottles.t_degc", 
  species_group = NULL,
  aoi_wkt       = NULL, 
  depth_m_min   = NULL, 
  depth_m_max   = NULL,
  date_beg      = NULL, 
  date_end      = NULL, 
  time_step     = "year",
  stats = c("avg", "sd")){
  # TODO: ∆ `var = NULL,` to `var,` and use `!missing(var)` vs `!is.null(var)`
  #         per https://community.rstudio.com/t/default-required-true-value-for-endpoint-parameters-in-plumber-r-package/130474
  
  # DEBUG
  # variable = "ctd_bottles.t_degc"
  # aoi_sf <- st_read(con, "aoi_fed_sanctuaries") %>%
  #   filter(nms == "CINMS")
  # aoi_wkt <- aoi_sf %>%
  #   pull(geom) %>%
  #   st_as_text()
  # st_bbox(aoi_sf) %>% st_as_sfc() %>% st_as_text()
  # variable = "larvae_counts.count"
  # species_group = "Anchovy"
  # variable = "ctd_bottles.t_degc"
  # aoi_wkt <- "POLYGON ((-120.6421 33.36241, -118.9071 33.36241, -118.9071 34.20707, -120.6421 34.20707, -120.6421 33.36241))"
  # date_beg = NULL; date_end = NULL
  # time_step = "year"
  # stats = "p10, mean, p90"
  # depth_m_min = NULL; depth_m_max = NULL

  # aoi_sf <- st_read(con, "aoi_fed_sanctuaries") %>%
  #   filter(nms == "CINMS")
  # aoi_wkt <- aoi_sf %>%
  #   pull(geom) %>%
  #   st_as_text()
  
  # variable = "ctd_bottles.t_degc"
  # aoi_wkt = NULL
  # depth_m_min = 0
  # depth_m_max = 5351
  # date_beg = "1949-02-28"
  # date_end = "2020-01-26"
  # time_step = "year"
  # stats = c("mean", "sd")
  
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
    v$tbl == "ctd_bottles"    ~ "ctd_casts JOIN ctd_bottles USING (cast_count)",
    v$tbl == "ctd_dic"        ~ "ctd_casts JOIN ctd_bottles USING (cast_count) JOIN ctd_dic USING (btl_cnt)",
    v$tbl == "larvae_counts"  ~ "larvae_counts 
        JOIN tows USING (cruise, ship, orderocc, towtype, townum, netloc)
        JOIN stations USING (cruise, ship, orderocc)
        LEFT JOIN species_groups USING (spccode)")

  tbl.geom <- case_when(
    v$tbl %in% c("ctd_bottles", "ctd_dic") ~  "ctd_casts.geom",
    v$tbl == "larvae_counts" ~ "stations.geom")
  
  q_where_aoi = ifelse(
    !is.null(aoi_wkt),
    glue("ST_Intersects(ST_GeomFromText('{aoi_wkt}', 4326), {tbl.geom})"),
    "TRUE")

  fxn_where_date <- function(date_beg, date_end){
    if (!is.null(date_beg) & !is.null(date_end))
      return(glue2("date >= '{date_beg}' AND date <= '{date_end}'"))
    if (is.null(date_beg)  & !is.null(date_end))
      return(glue2("date <= '{date_end}'"))
    if (!is.null(date_beg) &  is.null(date_end))
      return(glue2("date >= '{date_beg}'"))
    # else is.null(date_beg) & is.null(date_end)
    "TRUE"
  }
  q_where_date = fxn_where_date(date_beg, date_end)


  fxn_where_depth <- function(depth_m_min, depth_m_max){
    if (!is.null(depth_m_min) & !is.null(depth_m_max))
      return(glue2("{v$tbl}.depth_m >= {depth_m_min} AND {v$tbl}.depth_m <= {depth_m_max}"))
    if (is.null(depth_m_min) & !is.null(depth_m_max))
      return(glue2("{v$tbl}.depth_m <= {depth_m_max}"))
    if (!is.null(depth_m_min) &  is.null(depth_m_max))
      return(glue2("{v$tbl}.depth_m >= {depth_m_min}"))
    # else is.null(depth_m_min) & is.null(depth_m_max)
    "TRUE"
  }
  q_where_depth = fxn_where_depth(depth_m_min, depth_m_max)
  
  q_where_species_group = "TRUE"
  
  # special case of Larvae
  # TODO: add starboard filter
  if (v$tbl == "larvae_counts"){
    # larval tows only at surface, so turn off depth filter
    q_where_depth = "TRUE"
    
    # offset by percsorted (increasing count if < 100%) and divide by unit effort
    v$fld = "count / percsorted / volsampled"
    
    if (!is.null(species_group)){
      # check species_group is valid
      spp <- tbl(con, "species_groups") %>% filter(group == species_group) %>% pull(spccode)
      if (length(spp) == 0 ) stop("species_group invalid -- returns 0 species") 
      
      q_where_species_group = glue("spp_group = '{species_group}'")
    }
  }
  
  # TODO: get median, percentile ----
  # https://leafo.net/guides/postgresql-calculating-percentile.html
  # https://www.postgresql.org/docs/9.4/functions-aggregate.html
  
  q <- glue(
    "SELECT 
      {q_time_step} AS {time_step}, 
      AVG({v$fld}) AS val_avg, STDDEV({v$fld}) AS val_sd, 
      COUNT(*) AS n_obs
    FROM {q_from}
    WHERE {q_where_aoi} AND {q_where_date} AND {q_where_depth} AND {q_where_species_group}
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
  
  # d %>% readr::write_csv("tmp_larvae.csv")
  
  d
}

# /timeseries ----
#* Get time series data
#* @param variable:str The variable of interest. Must be one listed in `/variables`.
#* @param aoi_wkt:str Area of Interest (AOI) spatially described as well known text (WKT). Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_min:int Depth (meters) minimum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_max:int Depth (meters) maximum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_beg:str Date to begin, e.g. "2000-01-01". Defaults to NULL, i.e. no filter or entire dataset.
#* @param date_end:str Date to end, e.g. "2020-12-31". Defaults to NULL, i.e. no filter or entire dataset.
#* @param time_step:str time step over which to summarize; one of: a sequential increment ("decade", "year", "year.quarter", "year.month", "year.week", "date") or a climatology ("quarter","month","week","julianday","hour"). default is "year". If NULL then all values are returned, i.e. no aggregation by `time_step` or `stats` are applicable.
#* @param stats:[str] Statistics to show per `time_step`. Defaults to "p05", "avg", "p95". Acceptable comma-separated values include: "avg", "median", "min", "max", "sd" or "p#" where "sd" is the standard deviation and "p#" represents the percentile value 0 to 100 within available range of values for given aggregated `time_step`.
#* @get /timeseries
#* @serializer csv
function(
    variable = "ctd_bottles.t_degc", 
    aoi_wkt = NULL, 
    depth_m_min = NULL, depth_m_max = NULL,
    date_beg = NULL, date_end = NULL, 
    time_step = "year",
    stats = c("p10", "avg", "p90")){
  
  # DEBUG
  # variable = "ctd_bottles.t_degc"
  # aoi_sf <- st_read(con, "aoi_fed_sanctuaries") %>%
  #   filter(nms == "CINMS")
  # aoi_wkt <- aoi_sf %>%
  #   pull(geom) %>%
  #   st_as_text()
  # st_bbox(aoi_sf) %>% st_as_sfc() %>% st_as_text()
  # aoi_wkt <- "POLYGON ((-120.6421 33.36241, -118.9071 33.36241, -118.9071 34.20707, -120.6421 34.20707, -120.6421 33.36241))"
  # date_beg = NULL; date_end = NULL
  # time_step = "year"
  # stats = "p10, mean, p90"
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
    v$tbl == 'ctd_bottles'     ~ "ctd_casts JOIN ctd_bottles USING (cast_count)",
    v$tbl == 'ctd_bottles_dic' ~ "ctd_casts JOIN ctd_bottles USING (cast_count) JOIN ctd_dic USING (btl_cnt)")
  
  q_where_aoi = ifelse(
    !is.null(aoi_wkt),
    glue("ST_Intersects(ST_GeomFromText('{aoi_wkt}', 4326), ctd_casts.geom)"),
    "TRUE")
  
  q_where_date = case_when(
    !is.null(date_beg) & !is.null(date_end) ~ glue2("date >= '{date_beg}' AND date <= '{date_end}'"),
    is.null(date_beg) & !is.null(date_end) ~ glue2("date <= '{date_end}'"),
    !is.null(date_beg) &  is.null(date_end) ~ glue2("date >= '{date_beg}'"),
    TRUE ~ "TRUE")
  
  q_where_depth = case_when(
    !is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m >= {depth_m_min} AND depth_m <= {depth_m_max}"),
    is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m <= {depth_m_max}"),
    !is.null(depth_m_min) &  is.null(depth_m_max) ~ glue2("depth_m >= {depth_m_min}"),
    TRUE ~ "TRUE")
  
  # TODO: get median, percentile ----
  # https://leafo.net/guides/postgresql-calculating-percentile.html
  # https://www.postgresql.org/docs/9.4/functions-aggregate.html
  
  q <- glue(
    "SELECT {q_time_step} AS {time_step}, AVG({v$tbl}.{v$fld}) AS {v$fld}_avg, STDDEV({v$tbl}.{v$fld}) AS {v$fld}_sd, COUNT(*) AS n_obs
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


# /raster ----
#* Get raster of variable
#* @param variable:str The variable of interest. Must be one listed in `/variables`.
#* @param cruise_id:str The `cruise_id` identifier. Must be one listed in `/cruises`.
#* @param depth_m_min:int Depth (meters) minimum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_max:int Depth (meters) maximum. Defaults to NULL, i.e. no filter or entire dataset.
#* @get /raster
#* @serializer contentType list(type="image/tif")
function(variable = "ctd_bottles.t_degc", cruise_id = "2020-01-05-C-33RL", depth_m_min = 0, depth_m_max = 100){
  # @serializer tiff
  # test values
  # variable = "ctd_bottles.t_degc"; cruise_id = "2020-01-05-C-33RL"; depth_m_min = 0; depth_m_max = 10
  # variable = "ctd_bottles.t_degc"; cruise_id = "2020-01-05-C-33RL"; depth_m_min = 0; depth_m_max = 100
  # variable = "ctd_bottles_dic.bottle_o2_mmol_kg"; cruise_id = "1949-03-01-C-31CR"; variable = ""; depth_m_min = 0; depth_m_max = 1000
  # variable = "ctd_bottles.t_degc"; cruise_id = "2020-01-05-C-33RL"; depth_m_min = 0L; depth_m_max = 5351L
  # check input arguments ----
  
  args_in <- as.list(match.call(expand.dots=FALSE))[-1]
  # debug:
  # args_in <- list(variable=variable, cruise_id=cruise_id, depth_m_min=depth_m_min, depth_m_max=depth_m_max)
  hash    <- digest(args_in, algo="crc32")
  f_tif   <- glue("{dir_cache}/api_raster_{hash}.tif")
  # f_tif <- "/tmp/api_raster_dd83f5a7.tif"
  
  if (file.exists(f_tif)){
    message(glue("reading from cache: {basename(f_tif)}"))
    readBin(f_tif, "raw", n = file.info(f_tif)$size) %>% 
      return()
  }
  
  # variable
  v <- tbl(con, "field_labels") %>% 
    filter(table_field == !!variable) %>% 
    collect() %>% 
    separate(table_field, into=c("tbl", "fld"), sep="\\.", remove=F)
  stopifnot(nrow(v) == 1)
  
  # construct SQL
  q_from <- case_when(
    v$tbl == "ctd_bottles" ~ "ctd_casts JOIN ctd_bottles USING (cast_count)",
    v$tbl == "ctd_dic"     ~ "ctd_casts JOIN ctd_bottles USING (cast_count) JOIN ctd_dic USING (btl_cnt)")
  
  q_where_depth = case_when(
    !is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m >= {depth_m_min} AND depth_m <= {depth_m_max}"),
     is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("depth_m <= {depth_m_max}"),
    !is.null(depth_m_min) &  is.null(depth_m_max) ~ glue2("depth_m >= {depth_m_min}"),
    TRUE ~ "TRUE")
  
  q <- glue(
    "SELECT 
      AVG({v$fld}) AS {v$fld}, 
      STDDEV({v$fld}) AS {v$fld}_sd, COUNT(*) AS n_obs,
      geom
    FROM {q_from}
    WHERE 
      {q_where_depth} AND
      cruiseid = '{cruise_id}'
    GROUP BY geom")
  message(q)
  pts_gcs <- st_read(con, query=q)
  
  # TODO: figure out why all points are repeating and if that makes sense
  # cruise_id = "2020-01-05-C-33RL"; variable = "ctd_bottles.t_degc"; depth_m_min = 0; depth_m_max = 10
  # table(pts$n_obs)
  #    3  4  5  6 
  #   52 36 13  2

  # transform from geographic coordinate system (gcs) 
  #   to web mercator (mer) for direct use with leaflet::addRasterImage()
  pts_mer <- st_transform(pts_gcs, 3857)
  h <- st_convex_hull(st_union(pts_mer)) %>% st_as_sf() %>% mutate(one = 1)
  # library(mapview); mapviewOptions(fgb = F); mapview(h)
  r <- raster(as_Spatial(h), res=1000, crs=3857)
  z <- rasterize(as_Spatial(h), r, "one")
  
  # inverse distance weighted interpolation
  #   https://rspatial.org/raster/analysis/4-interpolation.html
  
  
  # TODO: kriging, log/log10 transformations
  # IDW
  var <- sym(v$fld)
  frm <- eval(expr(!!var~1))
  gs <- gstat(formula=frm, locations=pts_mer, set = list(idp = .5), nmax=10)
  idw <- interpolate(z, gs)
  w <- mask(idw, z)
  
  message(glue("writing to: {basename(f_tif)}")) # api_raster_a0f732d3.tif
  writeRaster(w, f_tif, overwrite=T)
  readBin(f_tif, "raw", n = file.info(f_tif)$size)
}

# /cruise_lines ----
#* Get station lines from cruises (with more than one cast)
#* @param cruise_id:str The `cruise_id` identifier. Must be one listed in `/cruises`.
#* @get /cruise_lines
#* @serializer csv
function(cruise_id){
  cruises <- dbGetQuery(con, "SELECT DISTINCT cruiseid FROM ctd_casts") %>% 
    pull(cruiseid)
  # (cruise_id <- cruises[1])  # 2020-01-05-C-33RL
  stopifnot(cruise_id %in% cruises)

  # get casts, filtering by cruise
  casts <- tbl(con, "ctd_casts") %>% 
    filter(cruiseid == !!cruise_id) %>% 
    select(cast_count, sta_id, date, longitude, latitude) %>% 
    collect() %>% 
    separate(
      sta_id, into = c("sta_line", "sta_offshore"), 
      sep = " ", convert = T, remove = F) %>% 
    mutate(
      day = difftime(date, min(date), units="days") %>% 
        as.integer()) # %>% 
    # st_as_sf(
    #   coords = c("longitude", "latitude"),
    #   crs = 4326, remove = F)
  
  # mapview(casts, zcol="sta_line")
  # mapview(casts, zcol="sta_offshore")
  # mapview(casts, zcol="day")
  
  # table(casts$sta_line)
  
  # remove station lines with only one cast
  casts <- casts %>% 
    group_by(sta_line) %>% 
    mutate(sta_line_n = n()) %>% 
    filter(sta_line_n > 1)
  
  # table(casts$sta_line)
  casts
}

# /cruise_line_profile ----
#* Get profile at depths for given variable of casts along line of stations
#* @param cruise_id:str The `cruise_id` identifier. Must be one listed in `/cruises`.
#* @param sta_line:numeric The `sta_line` identifying the alongshore component of the station id. Must be one listed in `/cruise_line`.
#* @param variable:str The variable of interest. Must be one listed in `/variables`.
#* @get /cruise_line_profile
#* @serializer csv
function(cruise_id, sta_line, variable){
  # cruise_id='2020-01-05-C-33RL'; sta_line = 60; variable = "ctd_bottles.t_degc"
  
  cruises <- dbGetQuery(con, "SELECT DISTINCT cruiseid FROM ctd_casts") %>% 
    pull(cruiseid)
  # (cruise_id <- cruises[1]) # "2020-01-05-C-33RL"
  stopifnot(cruise_id %in% cruises)
  
  cruise_lines <- tbl(con, "ctd_casts") %>% 
    filter(cruiseid == !!cruise_id) %>% 
    select(cast_count, sta_id, date, longitude, latitude) %>% 
    collect() %>% 
    separate(
      sta_id, into = c("sta_line", "sta_offshore"), 
      sep = " ", convert = T, remove = F) %>% 
    mutate(
      day = difftime(date, min(date), units="days") %>% 
        as.integer()) %>% 
    group_by(sta_line) %>% 
    mutate(sta_line_n = n()) %>% 
    filter(sta_line_n > 1) %>% 
    distinct(sta_line) %>% 
    pull(sta_line)
  # (sta_line <- cruise_lines[1]) # 60
  stopifnot(sta_line %in% cruise_lines)
  
  # variable
  v <- tbl(con, "field_labels") %>% 
    filter(table_field == !!variable) %>% 
    collect() %>% 
    separate(table_field, into=c("tbl", "fld"), sep="\\.", remove=F)
  stopifnot(nrow(v) == 1)
  
  # get casts, filtering by cruise
  casts <- tbl(con, "ctd_casts") %>% 
    filter(cruiseid == !!cruise_id) %>% 
    select(cast_count, sta_id, date, longitude, latitude) %>% 
    collect() %>% 
    separate(
      sta_id, into = c("sta_line", "sta_offshore"), 
      sep = " ", convert = T, remove = F) %>% 
    mutate(
      day = difftime(date, min(date), units="days") %>% 
        as.integer()) %>%
    mutate(sta_line_n = n()) %>% 
    filter(sta_line_n > 1) %>% 
    # filter by station line
    filter(sta_line == !!sta_line)
  
  bottles <- dbGetQuery(
    con,
    glue(
      "SELECT cast_count, depth_m, {v$fld} 
      FROM ctd_bottles
      WHERE cast_count IN ({paste(casts$cast_count, collapse=',')})"))
  d <- casts %>%
    inner_join(
      bottles,
      by="cast_count") %>% 
    # st_drop_geometry() %>% 
    # select(sta_offshore, sta_line, depth_m, t_degc) %>% 
    arrange(sta_offshore, sta_line, depth_m)
  d
}

# / home redirect ----
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
