#######################
# Author: Rodrigo Orellana
# Date: 28-03-2020
# Description:  View Weather prediction ARIMA. input: collection{'hour','humidity','temperature'}
#######################


from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)
mongo_client=MongoClient('mongodb://mongo:27017/')
#mongo_client = MongoClient('mongodb://localhost:27017/')


@app.route('/servicio/v1/prediccion/24horas/')
def prediction24():

	db = mongo_client.weather
	collection = db.prediction
	data = collection.find({"hour": {"$lt": 24}})
	output = []
	for record in data: 
		dic = {}
		dic["hour"] = record['hour']
		dic["temperature"] = record['temperature']
		dic["humidity"] = record['humidity']
		output.append(dic)
	return jsonify(output)


@app.route('/servicio/v1/prediccion/48horas/')
def prediction48():
	
	db = mongo_client.weather
	collection = db.prediction
	data = collection.find({"hour": {"$lt": 48}})
	output = []
	for record in data: 
		dic = {}
		dic["hour"] = record['hour']
		dic["temperature"] = record['temperature']
		dic["humidity"] = record['humidity']
		output.append(dic)
	return jsonify(output)

@app.route('/servicio/v1/prediccion/72horas/')
def prediction72():
	
	db = mongo_client.weather
	collection = db.prediction
	data = collection.find({"hour": {"$lt": 72}})
	output = []
	for record in data: 
		dic = {}
		dic["hour"] = record['hour']
		dic["temperature"] = record['temperature']
		dic["humidity"] = record['humidity']
		output.append(dic)
	return jsonify(output)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001, debug=True)
