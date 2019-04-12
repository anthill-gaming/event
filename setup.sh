#!/usr/bin/env bash

# Setup postgres database
createuser -d anthill_event -U postgres
createdb -U anthill_event anthill_event