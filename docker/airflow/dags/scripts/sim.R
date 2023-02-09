# options(repos=c(CRAN="https://cloud.r-project.org/bin/windows/contrib/4.2/remotes_2.4.2.zip")) # nolint
# install.packages(c("devtools", "RPostgres", "remotes")) # nolint

rm(list = ls(all = TRUE))

remotes::install_github("rfsaldanha/microdatasus")

library(DBI)
library(microdatasus)

dados <- fetch_datasus(
    year_start = 2010,
    year_end = 2020,
    uf = "ES",
    information_system = "SIM-DO")

dados <- process_sim(data)

tryCatch({{
    print("Connecting to Database…")

    con <- dbConnect(
        RPostgres::Postgres(),
        dbname = "{dbname}",
        host = "{host}",
        port = "{port}",
        user = "{user}",
        password = "{password}")

    print("Database Connected!")
}},
error = function(cond) {{
    print("Unable to connect to Database.")
}})

dbWriteTable(conn=con, name=Id(schema="{schema}", table="{table_name}"), value=dados, overwrite=TRUE) # nolint
dbDisconnect(con)
print("Carga finalizada com sucesso!")