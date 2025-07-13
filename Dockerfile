FROM python:3.13.5-alpine
COPY . .
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "src/Main.py", "config/config.yaml"]