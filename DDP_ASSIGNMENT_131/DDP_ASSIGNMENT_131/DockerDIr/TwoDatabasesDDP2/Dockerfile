# Use the existing image as the base image
FROM ddp-node

# Specify the volumes to inherit
VOLUME /volumes/database1
VOLUME /volumes/database2

# Set the working directory
WORKDIR /app

COPY . /app

RUN javac -cp .:sqlite-jdbc-3.42.0.0.jar manipulateData.java HashJoin.java pipelineHashJoin.java semiJoin.java
RUN apt-get update && apt-get install -y sqlite3

CMD java -cp .:sqlite-jdbc-3.42.0.0.jar manipulateData