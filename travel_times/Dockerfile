ARG BUILD_FROM=ghcr.io/home-assistant/base:3.24
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip tzdata \
 && pip3 install --no-cache-dir --break-system-packages \
      flask==3.0.3 requests==2.32.3 paho-mqtt==1.6.1 "curl_cffi>=0.16.3"

COPY app /app
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
