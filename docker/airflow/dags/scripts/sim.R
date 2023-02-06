options(repos=c(CRAN='http://cran.rstudio.com'))

#install.packages(c('devtools', 'RPostgres', 'remotes'), dependencies=TRUE)
#remotes::install_github('rfsaldanha/microdatasus')

library(DBI)
library(microdatasus)

dados <- fetch_datasus(
  year_start=2010, 
  year_end=2020, 
  uf='ES', 
  information_system='SIM-DO'
)

tryCatch({{
  print('Connecting to Database…')

  con <- dbConnect(
    RPostgres::Postgres(),
    dbname='saude_mental',
    host='localhost',
    port='5432',
    user='postgres',
    password='postgres')

  print('Database Connected!')
}},
error=function(cond) {{
  print('Unable to connect to Database.')
}})

dbWriteTable(conn=con, name=Id(schema='stg', table='sim_2010_2020'), value=dados, overwrite=TRUE)
dbDisconnect(con)
print('Carga finalizada com sucesso!')
