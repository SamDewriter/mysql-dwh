FROM python:3.9

RUN apt-get install wget
COPY . . 
RUN pip3 install -r requirements.txt

WORKDIR /app