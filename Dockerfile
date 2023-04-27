FROM python:3.10.11-bullseye
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
EXPOSE 5000
CMD python src/main.py