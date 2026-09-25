FROM python:3.11-slim

# JRE 17 récupérée depuis l'image officielle Temurin : deux fois plus légère à
# télécharger qu'un "apt install openjdk-17-jre-headless", et versionnée.
# Python 3.11 (et non 3.12+) : c'est la version sur laquelle PySpark 3.5 est éprouvé.
COPY --from=eclipse-temurin:17-jre-noble /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# procps : Spark appelle "ps" au démarrage, sans lui chaque job s'ouvre sur une erreur
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Driver JDBC PostgreSQL embarqué dans l'image : pas de --packages, qui exigerait
# un accès à Maven Central à chaque exécution du job.
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /opt/jars/postgresql.jar
RUN chmod 644 /opt/jars/postgresql.jar

COPY *.py ./

ENV PYTHONUNBUFFERED=1
CMD ["python", "aggregate.py"]
