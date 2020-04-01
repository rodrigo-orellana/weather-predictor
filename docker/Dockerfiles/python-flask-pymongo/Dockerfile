FROM       python:3.7
MAINTAINER Rodrigo Orellana <rodrigo.usm@gmail.com>

## Flask Extensions
RUN pip3 install flask
RUN pip3 install pymongo

EXPOSE 8001

ENTRYPOINT ["python" , "/app1/prediction_visor.py"]
