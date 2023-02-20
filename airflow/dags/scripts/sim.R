#install.packages(c("devtools", "RPostgres", "remotes"), dependencies=TRUE) # nolint
remotes::install_github("rfsaldanha/microdatasus", dependencies = TRUE)

library(DBI)
library(microdatasus)

dados <- fetch_datasus(year_start = 2010, year_end = 2020, uf = "ES", information_system = "SIM-DO") # nolint
dados <- process_sim(dados)

#tryCatch({{
print("Connecting to Database…")

con <- DBI::dbConnect(RPostgres::Postgres(), dbname = "saude_mental", host = "172.18.0.2", port = "5432", user = "postgres", password = "postgres") # nolint

print("Database Connected!")

dbWriteTable(conn=con, name=Id(schema="stg", table="sim_2010_2020"), value=dados, overwrite=TRUE) # nolint
dbDisconnect(con)
print("Carga finalizada com sucesso!")
#}},
#error = function(cond) {{
#  print("Unable to connect to Database.")
#}})
