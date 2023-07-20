FROM openjdk:11

WORKDIR /app

COPY . /app

RUN javac -cp .:sqlite-jdbc-3.42.0.0.jar twoDatabases.java
RUN apt-get update && apt-get install -y sqlite3

CMD java -cp .:sqlite-jdbc-3.42.0.0.jar twoDatabases
VOLUME ["volumes/database1"]
VOLUME ["volumes/database2"]