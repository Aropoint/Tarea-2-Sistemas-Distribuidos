FROM python:3.9-slim

WORKDIR /usr/src/app

COPY generator.py . 



RUN pip install --upgrade pip

RUN pip install --no-cache-dir requests numpy scipy


CMD ["python", "generator.py"]
