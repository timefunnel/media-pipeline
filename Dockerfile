FROM python:3.12-alpine

WORKDIR /app
COPY app/ /app/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "pipeline.cli"]
CMD ["folders"]
