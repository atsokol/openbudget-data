library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)
library(lubridate)
library(arrow)

source("download.R")

city_codes <- read_csv("inputs/city_codes.csv")

cities <- city_codes |> filter(city != "R_Vinnitsia") |> pull(city) |> unique()
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Download data
current_year <- year(Sys.Date())
data <- download_data(codes, seq(2021, current_year))

write_parquet(data[[1]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/credits.parquet")
write_parquet(data[[2]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/expenses.parquet")
write_parquet(data[[3]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/expenses_functional.parquet")
write_parquet(data[[4]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/expenses_program.parquet")
write_parquet(data[[5]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/debts.parquet")
write_parquet(data[[6]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data_parquet/incomes.parquet")

#Test
