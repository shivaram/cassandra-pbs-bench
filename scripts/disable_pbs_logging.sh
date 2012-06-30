#!/bin/bash

echo "run -b org.apache.cassandra.service:type=PBSPredictor disableConsistencyPredictionLogging" | java -jar jmxterm-1.0-alpha-4-uber.jar -l localhost:7199
