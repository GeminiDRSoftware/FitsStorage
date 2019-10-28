FROM fitsimage:latest
LABEL maintainer="ooberdorf@gemini.edu"

RUN pip install ptvsd

EXPOSE 3000
EXPOSE 8000

ENV LOG_LEVEL DEBUG
ENV FITSSTORAGE_PTVSD True

#ENTRYPOINT ["python", "fits_storage/wsgihandler.py"]
ENTRYPOINT ["bash"]
#ENTRYPOINT ["/opt/modwsgi-default/apachectl", "-D", "FOREGROUND"]
