FROM python:3.12

# Update bundled libs
RUN apt-get update
RUN apt-get upgrade -y

# Install Java (for pyspark)
RUN apt-get install -y openjdk-21-jdk-headless

# Clone & build osm-parquetizer
RUN apt-get install -y maven
RUN git clone https://github.com/adrianulbona/osm-parquetizer.git
WORKDIR /osm-parquetizer
RUN ls
RUN mvn clean package
RUN mv /osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar /osm-parquetizer.jar

# Remove files required only for build
RUN apt-get remove -y maven
WORKDIR /
RUN rm -r /osm-parquetizer

# Copy over and install requirements
COPY dist/fell_loader_requirements.txt /requirements.txt
RUN pip install -r requirements.txt --timeout 120 --verbose

# TODO: Compile packages and add bundle to image
# This step disabled while debugging, files will be mounted via compose.yaml
# Copy package content
# COPY packages /fell_finder/packages
# COPY src/fell_finder_app /fell_finder/src/fell_finder_app

# Make package contents importable
RUN mkdir /fell_finder
WORKDIR /fell_finder
ENV PYTHONPATH=/fell_finder/packages:/fell_finder/src

# NOTE: It is assumed that the source data will be available under
#       the /ff_data mount point
ENV FF_DATA_DIR=/ff_data
ENV FF_PARQUETIZER_LOC=/osm-parquetizer.jar

# Spark configuration (high volume of temp data may be generated, assume ff_data has sufficient space)
ENV SPARK_LOCAL_DIR=/ff_data/temp/spark

# Run the ingestion script
CMD ["python", "src/fell_finder_app/ingestion.py"]