FROM python:3.13.5-alpine
COPY . /
RUN pip install -r requirements.txt
ENV PYTHONPATH=$PYTHONPATH:/src
WORKDIR /src
ENTRYPOINT ["python", "Main.py", "data/config/config.yaml"]