# CalCOFI API endpoints

# packages ----
if (!require("librarian")){
  install.packages("librarian")
  library(librarian)
}
librarian::shelf(
  DBI, dbplyr, digest, dplyr, glue, gstat, here, lubridate, 
  plumber, raster, RPostgres, sf, stringr, tidyr)
select = dplyr::select

# paths ----
db_pass_txt <- "~/.calcofi_db_pass.txt"
dir_cache <- "/tmp"

# database ----
stopifnot(file.exists(db_pass_txt))

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = "gis",
  host     = "db.calcofi.io",
  port     = 5432,
  user     = "admin",
  password = readLines(db_pass_txt))

# test db connection
# dbListTables(con)

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
    filter(active) %>% 
    select(-active) %>% 
    collect()
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
  stats = c("avg", "sd")){
  
  # DEBUG
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
    # v$tbl == 'ctdcast_bottle'     ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt)",
    # v$tbl == 'ctdcast_bottle_dic' ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt) JOIN ctdcast_bottle_dic USING (btl_cnt)")
    v$tbl == 'ctd_bottles'     ~ "ctd_casts JOIN ctd_bottles     ON ctd_casts.cast_count = ctd_bottles.cst_cnt",
    v$tbl == 'ctd_bottles_dic' ~ "ctd_casts JOIN ctd_bottles_dic ON ctd_casts.cast_count = ctd_bottles_dic.cst_cnt")

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
    !is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("{v$tbl}.depthm >= {depth_m_min} AND {v$tbl}.depthm <= {depth_m_max}"),
     is.null(depth_m_min) & !is.null(depth_m_max) ~ glue2("{v$tbl}.depthm <= {depth_m_max}"),
    !is.null(depth_m_min) &  is.null(depth_m_max) ~ glue2("{v$tbl}.depthm >= {depth_m_min}"),
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


# /cruises ----
#* Get list of cruises with summary stats as CSV table for time (`date_beg`)
#* @get /cruises
#* @serializer csv
function() {

  # TODO: add ctdcast indexes to db for: cruise_id, date, lon_dec, lat_dec
  tbl(con, "ctd_casts") %>% 
    group_by(cruiseid) %>% 
    summarize(
      date_beg = min(date, na.rm=T),
      date_end = max(date, na.rm=T),
      lon_min  = min(longitude, na.rm=T),
      lon_max  = max(longitude, na.rm=T),
      lat_min  = min(latitude, na.rm=T),
      lat_max  = max(latitude, na.rm=T),
      n_casts = n(),
      .groups = "drop") %>% 
    arrange(desc(cruiseid)) %>% 
    collect()
}

# /raster ----
#* Get raster of variable
#* @param variable:str The variable of interest. Must be one listed in `/variables`.
#* @param cruise_id:str The `cruise_id` identifier. Must be one listed in `/cruises`.
#* @param depth_m_min:int Depth (meters) minimum. Defaults to NULL, i.e. no filter or entire dataset.
#* @param depth_m_max:int Depth (meters) maximum. Defaults to NULL, i.e. no filter or entire dataset.
#* @get /raster
#* @serializer contentType list(type="image/tif")
function(variable = "ctdcast_bottle.t_deg_c", cruise_id = "2020-01-05-C-33RL", depth_m_min = 0, depth_m_max = 100){
  # @serializer tiff
  # test values
  # variable = "ctdcast_bottle.t_deg_c"; cruise_id = "2020-01-05-C-33RL"; depth_m_min = 0; depth_m_max = 10
  # variable = "ctdcast_bottle.t_deg_c"; cruise_id = "2020-01-05-C-33RL"; depth_m_min = 0; depth_m_max = 100
  # variable = "ctdcast_bottle_dic.bottle_o2_mmol_kg"; cruise_id = "1949-03-01-C-31CR"; variable = ""; depth_m_min = 0; depth_m_max = 1000
  
  # check input arguments ----
  
  args_in <- as.list(match.call(expand.dots=FALSE))[-1]
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
    v$tbl == 'ctdcast_bottle'     ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt)",
    v$tbl == 'ctdcast_bottle_dic' ~ "ctdcast JOIN ctdcast_bottle USING (cst_cnt) JOIN ctdcast_bottle_dic USING (btl_cnt)")
  
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
      cruise_id = '{cruise_id}'
    GROUP BY geom")
  message(q)
  pts_gcs <- st_read(con, query=q)
  
  # TODO: figure out why all points are repeating and if that makes sense
  # cruise_id = "2020-01-05-C-33RL"; variable = "ctdcast_bottle.t_deg_c"; depth_m_min = 0; depth_m_max = 10
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
