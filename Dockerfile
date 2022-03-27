FROM python:3.9

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

WORKDIR /app
COPY data_upload.py data_upload.py

ENTRYPOINT [ "python", "data_upload.py" ]