FROM python:3.8-slim-buster

COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt

WORKDIR /srv/twitterelk
COPY streamer.py .

CMD [ "python3", "./streamer.py" ]
