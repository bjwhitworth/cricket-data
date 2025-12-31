# Simple analysis of England's test runs totals in this dataset

# Libraries
library(dplyr)
library(ggplot2)

# list tables in the database
dbListTables(con)

# Attach data
stg_innings <- tbl(con, "stg_cricket__innings")
stg_matches <- tbl(con, "stg_cricket__matches")

england_runs <-
  stg_innings %>%
  inner_join(stg_matches, by = "match_id") %>%
  filter(batting_team == "England") %>%
  group_by(match_start_date, innings_number) %>%
  summarise(total_runs = sum(runs_total)) %>%
  arrange(match_start_date) %>%
  collect()

england_runs %>%
  ggplot(aes(x = match_start_date, y = total_runs, color = as.factor(innings_number))) +
  geom_line() +
  geom_point() +
  labs(title = "England Test Match Innings Runs Over Time",
       x = "Match Start Date",
       y = "Total Runs",
       color = "Innings Number") +
  facet_wrap(~innings_number) +
  theme_minimal()


# Grab min first innings runs for England
results <-
  dbGetQuery(con,
             "
             SELECT *
             FROM stg_cricket__innings
             WHERE batting_team = 'England'
               AND innings_number = 1
             ORDER BY runs_total ASC
             LIMIT 10")

print(results)
