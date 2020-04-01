FROM       python:3.7
MAINTAINER Rodrigo Orellana <rodrigo.usm@gmail.com>

## Flask Extensions
RUN pip3 install flask
RUN pip3 install bs4
RUN pip3 install requests


EXPOSE 8009

ENTRYPOINT ["python" , "/app2/scrapy_weather.py"]
