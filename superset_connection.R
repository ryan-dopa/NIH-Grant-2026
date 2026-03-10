# ==============================================================================
# superset_connection.R
# ------------------------------------------------------------------------------
# PURPOSE:   Authenticate with Apache Superset and download all four survey
#            dataset types as R data frames.
#
# DATASETS PULLED:
#   df_slider      – Perception Slider summary      (ID 1,  continuous)
#   df_knowledge   – Knowledge Question Responses   (ID 8,  proportion)
#   df_likert      – Likert Question Responses      (ID 7,  ordinal)
#   df_multiselect – Multi-Select Question Responses(ID 9,  categorical)
#
# SETUP:     Add to your project's .Renviron (usethis::edit_r_environ()):
#              SUPERSET_URL=http://dopage-bi-prod-alb-799515043.us-east-1.elb.amazonaws.com
#              SUPERSET_USER=your_username
#              SUPERSET_PASS=your_password
#            Then restart R so the variables load.
#
# DEPENDENCIES: httr, jsonlite, dplyr
# ==============================================================================


# ── 0. Install / load dependencies ────────────────────────────────────────────
required_pkgs <- c("httr", "jsonlite", "dplyr")
new_pkgs      <- required_pkgs[!sapply(required_pkgs, requireNamespace, quietly = TRUE)]
if (length(new_pkgs) > 0) install.packages(new_pkgs)
invisible(lapply(required_pkgs, library, character.only = TRUE))


# ── 1. USER CONFIGURATION ─────────────────────────────────────────────────────
SUPERSET_URL  <- Sys.getenv("SUPERSET_URL")
SUPERSET_USER <- Sys.getenv("SUPERSET_USER")
SUPERSET_PASS <- Sys.getenv("SUPERSET_PASS")

# Dataset IDs (confirmed from list_datasets() output)
SLIDER_DATASET_ID      <- 1    # Perception Slider summary
LIKERT_DATASET_ID      <- 7    # Survey - Likert Question Responses
KNOWLEDGE_DATASET_ID   <- 8    # Survey - Knowledge Question Responses
MULTISELECT_DATASET_ID <- 9    # Survey - Multi-Select Question Responses

# Row limit for all queries
ROW_LIMIT <- 10000

# Path for local CSV cache (enables offline / reproducible re-runs)
CACHE_DIR <- "data_cache"


# ── 2. VALIDATE CONFIGURATION ─────────────────────────────────────────────────
if (nchar(SUPERSET_URL) == 0)  stop("SUPERSET_URL not set. Add to .Renviron and restart R.")
if (nchar(SUPERSET_USER) == 0) stop("SUPERSET_USER not set. Add to .Renviron and restart R.")
if (nchar(SUPERSET_PASS) == 0) stop("SUPERSET_PASS not set. Add to .Renviron and restart R.")


# ── 3. AUTHENTICATION ─────────────────────────────────────────────────────────
#' Authenticate with Superset.
#' Returns a named list: $access_token, $csrf_token, $cookies (named vector)
superset_authenticate <- function(base_url, username, password) {

  message("Authenticating with Superset at: ", base_url)

  login_resp <- POST(
    url    = paste0(base_url, "/api/v1/security/login"),
    body   = list(username = username, password = password,
                  provider = "db", refresh = TRUE),
    encode = "json"
  )

  if (http_error(login_resp)) {
    stop("Login failed (", status_code(login_resp), "): ",
         content(login_resp, "text", encoding = "UTF-8"))
  }

  access_token <- content(login_resp, as = "parsed")$access_token
  if (is.null(access_token)) stop("No access token in login response.")

  # Collect all cookies from login (name=value named vector for set_cookies())
  login_ck <- cookies(login_resp)
  ck_vec   <- if (nrow(login_ck) > 0)
    setNames(login_ck$value, login_ck$name) else c()

  # Fetch CSRF token — pass Bearer + session cookie via set_cookies()
  csrf_resp <- GET(
    url    = paste0(base_url, "/api/v1/security/csrf_token/"),
    add_headers(Authorization = paste("Bearer", access_token)),
    do.call(set_cookies, as.list(ck_vec))
  )

  # The CSRF response may set a refreshed session cookie — collect it too
  csrf_ck <- cookies(csrf_resp)
  if (nrow(csrf_ck) > 0) {
    new_vec <- setNames(csrf_ck$value, csrf_ck$name)
    ck_vec  <- c(ck_vec[!names(ck_vec) %in% names(new_vec)], new_vec)
  }

  csrf_token <- ""
  if (!http_error(csrf_resp)) {
    csrf_token <- content(csrf_resp, as = "parsed")$result
    if (is.null(csrf_token)) csrf_token <- ""
  } else {
    warning("Could not retrieve CSRF token (", status_code(csrf_resp),
            "). POST requests may be rejected.")
  }

  message("Authentication successful.")
  list(
    access_token = access_token,
    csrf_token   = csrf_token,
    cookies      = ck_vec          # named vector: name → value
  )
}

#' Build auth config for every request: Bearer token + CSRF + cookies.
#' Returns a list with no NULLs — safe to splat with do.call().
auth_config <- function(auth) {
  cfg <- list(
    add_headers(
      Authorization  = paste("Bearer", auth$access_token),
      `X-CSRFToken`  = auth$csrf_token,
      `Content-Type` = "application/json",
      Referer        = SUPERSET_URL
    )
  )
  # Only append cookies config if cookies actually exist
  if (length(auth$cookies) > 0)
    cfg <- c(cfg, list(do.call(set_cookies, as.list(auth$cookies))))
  cfg
}


# ── 4. DATA FETCH FUNCTIONS ───────────────────────────────────────────────────

#' Fetch dataset rows via the Chart Data API (POST /api/v1/chart/data).
#' Does NOT require SQL Lab permissions.
#' Retrieves column names from dataset metadata first so the query isn't empty.
fetch_via_chart_api <- function(base_url, auth, dataset_id, row_limit = 10000) {

  message("  Trying Chart Data API for dataset ", dataset_id, "...")

  # Step 1: GET dataset metadata to discover column names
  meta_resp <- do.call(GET, c(
    list(url = paste0(base_url, "/api/v1/dataset/", dataset_id)),
    auth_config(auth)
  ))

  col_names <- character(0)
  if (!http_error(meta_resp)) {
    parsed    <- content(meta_resp, as = "parsed")$result
    meta_cols <- parsed$columns

    if (!is.null(meta_cols) && length(meta_cols) > 0) {
      # Try column_name first (physical tables), fall back to column_name or name
      col_names <- sapply(meta_cols, function(c) {
        cn <- c$column_name
        if (is.null(cn) || nchar(trimws(cn)) == 0) cn <- c$name
        cn
      })
      col_names <- col_names[!is.na(col_names) & nchar(trimws(col_names)) > 0]
      message("  Found ", length(col_names), " columns from metadata.")
    }

    # For virtual datasets, also try the 'metrics' field to find available columns
    if (length(col_names) == 0 && !is.null(parsed$metrics)) {
      message("  No columns via metadata; dataset may be virtual with no cached schema.")
    }
  } else {
    message("  Metadata fetch failed (", status_code(meta_resp), "): ",
            content(meta_resp, "text", encoding = "UTF-8"))
  }

  if (length(col_names) == 0) {
    warning("No columns found for dataset ", dataset_id,
            ". Chart API cannot run without column names. ",
            "Try SQL Lab mode or check that the dataset has been refreshed in Superset ",
            "(Data > Datasets > ... > Sync columns from source).")
    return(NULL)
  }

  # Step 2: POST chart data query with explicit column list.
  # Keep body minimal — extra empty objects (applied_time_extras, url_params)
  # cause Superset to throw "Error: Empty query?" on some versions.
  body <- list(
    datasource    = list(id = as.integer(dataset_id), type = "table"),
    force         = FALSE,
    result_format = "json",
    result_type   = "full",
    queries       = list(list(
      row_limit   = as.integer(row_limit),
      all_columns = as.list(col_names),
      metrics     = list(),
      filters     = list(),
      orderby     = list(),
      extras      = list(having = "", where = "")
    ))
  )

  resp <- do.call(POST, c(
    list(url    = paste0(base_url, "/api/v1/chart/data"),
         body   = body,
         encode = "json"),
    auth_config(auth)
  ))

  if (http_error(resp)) {
    warning("Chart Data API failed (", status_code(resp), "): ",
            content(resp, "text", encoding = "UTF-8"))
    return(NULL)
  }

  result <- content(resp, as = "parsed")

  if (is.null(result$result) || length(result$result) == 0 ||
      is.null(result$result[[1]]$data)) {
    warning("Chart Data API returned no data for dataset ", dataset_id, ".")
    return(NULL)
  }

  rows <- result$result[[1]]$data
  if (length(rows) == 0) {
    warning("Dataset ", dataset_id, " returned 0 rows via Chart Data API.")
    return(data.frame())
  }

  df <- bind_rows(lapply(rows, function(r) {
    as.data.frame(lapply(r, function(v) if (is.null(v)) NA else v),
                  stringsAsFactors = FALSE)
  }))
  message("  Chart API: ", nrow(df), " rows x ", ncol(df), " cols.")
  df
}


#' Fetch dataset rows via SQL Lab (POST /api/v1/sqllab/execute/).
#' Requires SQL Lab permissions in Superset. Falls back to chart API if 403.
fetch_via_sql <- function(base_url, auth, database_id, sql, row_limit = 10000) {

  message("  Trying SQL Lab for database ", database_id, "...")

  resp <- do.call(POST, c(
    list(url    = paste0(base_url, "/api/v1/sqllab/execute/"),
         body   = list(
           database_id    = database_id,
           sql            = sql,
           runAsync       = FALSE,
           select_as_cta  = FALSE,
           tmp_table_name = "",
           client_id      = paste0("r_", sample.int(1e6, 1))
         ),
         encode = "json"),
    auth_config(auth)
  ))

  if (status_code(resp) == 403) {
    message("  SQL Lab returned 403 Forbidden — your account may not have SQL Lab access.")
    return(NULL)
  }

  if (http_error(resp)) {
    warning("SQL Lab failed (", status_code(resp), "): ",
            content(resp, "text", encoding = "UTF-8"))
    return(NULL)
  }

  result <- content(resp, as = "parsed")

  if (is.null(result$data) || length(result$data) == 0) {
    warning("SQL Lab returned 0 rows.")
    return(data.frame())
  }

  col_names <- sapply(result$columns, function(c) c$name)
  df <- as.data.frame(
    do.call(rbind, lapply(result$data, function(row) {
      sapply(row, function(v) if (is.null(v)) NA_character_ else as.character(v))
    })),
    stringsAsFactors = FALSE
  )
  colnames(df) <- col_names
  message("  SQL Lab: downloaded ", nrow(df), " rows, ", ncol(df), " columns.")
  df
}


#' Primary fetch function: gets dataset metadata, then tries SQL Lab first,
#' falls back to Chart Data API automatically.
fetch_dataset <- function(base_url, auth, dataset_id, row_limit = 10000) {

  message("Fetching dataset ID: ", dataset_id)

  # Step 1: Get metadata to find table name, schema, database ID
  meta_resp <- do.call(GET, c(
    list(url = paste0(base_url, "/api/v1/dataset/", dataset_id)),
    auth_config(auth)
  ))

  if (http_error(meta_resp)) {
    warning("Metadata fetch failed for dataset ", dataset_id,
            " (", status_code(meta_resp), "). Is the ID correct?")
    # Attempt chart API directly without metadata
    return(fetch_via_chart_api(base_url, auth, dataset_id, row_limit))
  }

  meta        <- content(meta_resp, as = "parsed")$result
  table_name  <- meta$table_name
  schema      <- meta$schema
  database_id <- meta$database$id
  dataset_sql <- trimws(if (!is.null(meta$sql)) meta$sql else "")

  # Step 2: Build SQL — virtual datasets (defined by a SQL query in Superset)
  # must use their own SQL wrapped as a subquery, NOT SELECT * FROM table_name,
  # because the table name is just a label and doesn't exist as a physical table.
  if (nchar(dataset_sql) > 0) {
    # Virtual dataset: run its SQL directly — do NOT wrap in a subquery,
    # because Superset's SQL parser rejects ORDER BY inside subqueries.
    # Strip trailing semicolons, then append LIMIT at the very end.
    message("  Virtual dataset — using Superset-defined SQL directly")
    sql_clean <- trimws(gsub(";\\s*$", "", dataset_sql))
    sql <- paste0(sql_clean, "\nLIMIT ", row_limit, ";")
  } else {
    # Physical table: double-quote to handle spaces and mixed case (PostgreSQL)
    quoted_table  <- paste0('"', gsub('"', '""', table_name), '"')
    quoted_schema <- if (!is.null(schema) && nchar(schema) > 0)
      paste0('"', gsub('"', '""', schema), '"') else NULL
    qualified <- if (!is.null(quoted_schema))
      paste0(quoted_schema, ".", quoted_table) else quoted_table
    message("  Physical table: ", qualified)
    sql <- paste0("SELECT * FROM ", qualified, " LIMIT ", row_limit, ";")
  }
  df  <- fetch_via_sql(base_url, auth, database_id, sql, row_limit)

  # Step 3: Fall back to Chart Data API if SQL Lab failed/forbidden
  if (is.null(df)) {
    message("  Falling back to Chart Data API...")
    df <- fetch_via_chart_api(base_url, auth, dataset_id, row_limit)
  }

  df
}


#' List all datasets visible to the authenticated user.
#' Run interactively to discover dataset IDs: list_datasets(SUPERSET_URL, auth)
list_datasets <- function(base_url, auth) {
  resp <- do.call(GET, c(
    list(url   = paste0(base_url, "/api/v1/dataset/"),
         query = list(q = '{"page_size":100}')),
    auth_config(auth)
  ))
  if (http_error(resp)) {
    warning("Could not list datasets (", status_code(resp), ").")
    return(invisible(NULL))
  }
  result <- content(resp, as = "parsed")$result
  df <- data.frame(
    id         = sapply(result, function(x) x$id),
    table_name = sapply(result, function(x) x$table_name),
    schema     = sapply(result, function(x) ifelse(is.null(x$schema), "", x$schema)),
    database   = sapply(result, function(x) x$database$database_name),
    stringsAsFactors = FALSE
  )
  message("Available datasets:")
  print(df)
  invisible(df)
}


# ── 5. CACHE HELPERS ──────────────────────────────────────────────────────────
save_cache <- function(df, name) {
  if (!is.null(df) && nrow(df) > 0) {
    path <- file.path(CACHE_DIR, paste0(name, ".csv"))
    write.csv(df, path, row.names = FALSE)
    message("Cached to: ", path)
  }
}

load_cache <- function(name) {
  path <- file.path(CACHE_DIR, paste0(name, ".csv"))
  if (!file.exists(path)) stop("No cached file found: ", path,
                               "\nRun with Superset connection to download first.")
  df <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  message("Loaded from cache: ", path, " (", nrow(df), " rows)")
  df
}


# ── 6. MAIN EXECUTION ─────────────────────────────────────────────────────────
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive = TRUE)

dataset_map <- list(
  slider      = SLIDER_DATASET_ID,
  knowledge   = KNOWLEDGE_DATASET_ID,
  likert      = LIKERT_DATASET_ID,
  multiselect = MULTISELECT_DATASET_ID
)

tryCatch({

  auth <- superset_authenticate(SUPERSET_URL, SUPERSET_USER, SUPERSET_PASS)

  for (name in names(dataset_map)) {
    df <- fetch_dataset(SUPERSET_URL, auth, dataset_map[[name]], ROW_LIMIT)
    save_cache(df, name)
    assign(paste0("df_", name), df, envir = .GlobalEnv)
  }

}, error = function(e) {

  warning("Could not connect to Superset: ", conditionMessage(e),
          "\nLoading from local cache...")

  for (name in names(dataset_map)) {
    assign(paste0("df_", name), load_cache(name), envir = .GlobalEnv)
  }
})

message("\nData objects ready in global environment:")
for (name in names(dataset_map)) {
  obj <- get(paste0("df_", name))
  message(sprintf("  df_%-12s : %d rows x %d cols",
                  name, nrow(obj), ncol(obj)))
}
