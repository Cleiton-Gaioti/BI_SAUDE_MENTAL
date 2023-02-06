#!/usr/bin/env Rscript
# shellcheck disable=SC2034
args=commandArgs(trailingOnly=TRUE)
setwd(dirname(args[1]))
source(args[1])