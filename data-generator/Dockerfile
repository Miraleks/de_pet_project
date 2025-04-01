FROM python:3.12-slim

RUN apt-get update && apt-get install -y cron

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY generate_data.py .
COPY generate_cron.sh .

RUN chmod +x generate_cron.sh

ARG CRON_SCHEDULE="0 10-20 * * *"
#ARG CRON_SCHEDULE="* * * * *"
RUN echo "$CRON_SCHEDULE /app/generate_cron.sh >> /var/log/cron.log 2>&1" > /etc/cron.d/data-cron


RUN chmod 0644 /etc/cron.d/data-cron && crontab /etc/cron.d/data-cron

CMD ["cron", "-f"]