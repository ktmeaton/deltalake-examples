#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(sparklyr))

message(paste(replicate(80, "-")))
df <- data.frame( list(sample=c("A", "B"), year=c(2023, 2024)))

pysparklyr::spark_connect_service_start("3.5")
# spark_home <- file.path(Sys.getenv("CONDA_PREFIX"), "lib", "python3.12", "site-packages", "pyspark")
# print(spark_home)
# print(Sys.getenv("SPARK_HOME"))
# sparklyr::spark_home_set(path = spark_home) 
# print(Sys.getenv("SPARK_HOME"))
# sparklyr::spark_install_find()
#sc <- spark_connect(master = "local", version="3.5.1")
#spark_read_delta(sc, "data/python")