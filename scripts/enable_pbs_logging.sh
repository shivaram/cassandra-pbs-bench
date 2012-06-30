#!/bin/bash

echo "run -b org.apache.cassandra.service:type=PBSPredictor enableConsistencyPredictionLogging" | java -jar $HOME/jmxterm-1.0-alpha-4-uber.jar -l localhost:7199
