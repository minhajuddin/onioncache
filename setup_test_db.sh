#!/bin/bash

# Configuration
echo "Setting up database"
psql --echo-all --file=./testdb.sql
