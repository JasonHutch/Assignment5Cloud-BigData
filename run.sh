#!/bin/bash

# Use PORT environment variable if set, else default to 8000
PORT=${PORT:-8000}

streamlit run assignmentFive.py --server.port $PORT --server.address 0.0.0.0
