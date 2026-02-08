FROM eclipse-temurin:17-jre

# Python is required for PythonFunction execution (UDFs + inline requirements.txt).
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY target/vertx-1.0-SNAPSHOT.jar /app/vertx.jar
ENV VERTX_MODE=operator
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/vertx.jar"]
