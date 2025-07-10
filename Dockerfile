FROM python:3.13.5-alpine
COPY . .
WORKDIR /src
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "main.py"]