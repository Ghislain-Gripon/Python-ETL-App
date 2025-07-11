FROM python:3.13.5-alpine
COPY . .
RUN pip install -r requirements.txt
WORKDIR /src
ENTRYPOINT ["python", "Main.py", "../config/config.yaml"]