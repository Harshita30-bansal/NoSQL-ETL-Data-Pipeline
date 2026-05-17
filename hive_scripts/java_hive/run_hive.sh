#!/bin/bash
cd "$(dirname "$0")"
javac HiveAnalytics.java
java -cp . HiveAnalytics "$@"
