#!/bin/bash

echo "== Setting up crm_erp database with Data Warehouse Dataset =="

# Use host connection instead of socket
export PGPASSWORD="6666"

echo "* create database and tables *" 
psql -h localhost -U postgres -f /home/ldduc/D/data-warehouse-apache-ecosystem/src/initial/source_db.sql

echo "-- create successfully --"

echo "* Load CSV files into database *"
psql -h localhost -U postgres -f /home/ldduc/D/data-warehouse-apache-ecosystem/src/initial/load_dataset.sql

echo "-- Load successfully --"
echo " == Database ready with data =="

echo ""
echo "== Data Warehouse source database setup completed =="