FROM python:3.9

COPY requirements.txt /opt/app/
RUN pip3 install -r /opt/app/requirements.txt

COPY decoder.py /opt/app/
WORKDIR /opt/app/
CMD ["python", "decoder.py"]