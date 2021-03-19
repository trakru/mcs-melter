FROM python:3.8-slim-buster

WORKDIR /Users/atrakru/Documents/mcs-melter

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "./utils/mcs-melter_local.py"]