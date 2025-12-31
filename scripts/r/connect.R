library(duckdb)

# Define DuckDB directory
duckdir <- "data/duckdb/"

# Connect to DuckDB database in read-only mode
con <- dbConnect(duckdb::duckdb(),
                 dbdir = paste0(duckdir, "dev.duckdb"),
                 read_only = TRUE)

# install json extension
dbExecute(con, "INSTALL json")

# disconnect database
dbDisconnect(con, shutdown = TRUE)
