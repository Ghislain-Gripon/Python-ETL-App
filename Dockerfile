FROM python:3.13.5-alpine
COPY --exclude=/data/*.* --exclude=*.* . .
WORKDIR /src
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "main.py"]