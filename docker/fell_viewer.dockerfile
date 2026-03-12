FROM python:3.12

# Update bundled libs
RUN apt-get update
RUN apt-get upgrade -y

# Copy over and install requirements
COPY dist/fell_viewer_requirements.txt /requirements.txt
RUN pip install -r requirements.txt

# TODO: Compile packages and add bundle to image
# This step disabled while debugging, files will be mounted via compose.yaml
# Copy package content
# COPY packages /fell_viewer/packages
# COPY src/fell_finder_app /fell_viewer/src/fell_finder_app

# Make package contents importable
RUN mkdir /fell_finder
WORKDIR /fell_finder
ENV PYTHONPATH=/fell_viewer/packages:/fell_finder/src

# Open port for webapp
EXPOSE 8050

# Run Celery & Webapp
COPY docker/fell_viewer.sh ./fell_viewer.sh
RUN ["chmod", "+x", "./fell_viewer.sh"]
CMD ["./fell_viewer.sh"]