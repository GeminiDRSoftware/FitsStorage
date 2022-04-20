FROM fitsstorageutils:latest
LABEL maintainer="oliver.oberdorf@noirlab.edu"

ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS:/opt/GeminiCalMgr:/opt/FitsStorageDB:/opt/FitsStorageServices

# Setup static assets folder
RUN mkdir /opt/FitsStorageServices_static

# Setup Django Services Folder
COPY FitsStorageServices /opt/FitsStorageServices

# Install python requirements
RUN python3 -m pip install -r /opt/FitsStorageServices/requirements.txt

# Setup sqlite DB
RUN python3 /opt/FitsStorageServices/manage.py migrate

# Install static files
RUN python3 /opt/FitsStorageServices/manage.py collectstatic --no-input

#EXPOSE 8090

CMD ["/usr/bin/python3", "/opt/FitsStorageServices/manage.py", "runserver", "0.0.0.0:8090"]
#ENTRYPOINT ["bash"]
