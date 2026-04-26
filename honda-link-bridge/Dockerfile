ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip jq \
 && pip3 install --break-system-packages --no-cache-dir requests paho-mqtt astral

COPY honda_bridge.py /honda_bridge.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
