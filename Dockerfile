# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /bg-db-updater

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
#CMD ["python3", "app.py", "--host=0.0.0.0", "--port=8085"]
#CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
