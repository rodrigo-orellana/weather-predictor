#######################
# Author: Rodrigo Orellana
# Date: 28-03-2020
# Description:  Scrapy Weather prediction 
# pip install bs4, flask
#######################
from bs4 import BeautifulSoup
from flask import Flask, jsonify
import requests

app = Flask(__name__)

@app.route('/servicio/v2/prediccion/24horas/')
def prediction24():
	
	hour = 24
	html = getData()
	data_humidity = algorithmH(html)
	data_temperature = algorithmT(html)
	data_dict = doDict(data_humidity, data_temperature, hour)

	return jsonify(data_dict)

@app.route('/servicio/v2/prediccion/48horas/')
def prediction48():
	
	hour = 48
	html = getData()
	data_humidity = algorithmH(html)
	data_temperature = algorithmT(html)
	data_dict = doDict(data_humidity, data_temperature, hour)

	return jsonify(data_dict)

@app.route('/servicio/v2/prediccion/72horas/')
def prediction72():
	
	hour = 72
	html = getData()
	data_humidity = algorithmH(html)
	data_temperature = algorithmT(html)
	data_dict = doDict(data_humidity, data_temperature, hour)

	return jsonify(data_dict)

def getData():
	url = "https://www.eltiempo.es/granada.html?v=por_hora"
	req = requests.get(url)
	html = BeautifulSoup(req.text,"html.parser")
	return html	

def algorithmH(html):
	
	humidities = html.find_all('div', {'class':'m_table_weather_hour_detail_child m_table_weather_hour_detail_hum'})
	data_dict = []
	for i in humidities:
		result = i.find_all('span')
		data_dict.append(result[1].text)
	return data_dict	

def algorithmT(html):

	temperatures = html.find_all('div', {'class':'m_table_weather_hour_detail_pred'})
	data_dict = []
	for i in temperatures :
		result = i.find_all('span')
		for j in result:
			data_dict.append(j['data-temp'])
	return data_dict

def doDict(data_humidity, data_temperature, hour):
	data_dict = []
	if(hour==72):
		hour=71 #por falta de datos en fuente
	for i in range(0, hour):
		#print("data_temperature[i]",i)
		#print(data_temperature[i])
		cel_temp=float(data_temperature[i]) + 273.15      
		data = {}
		data["hour"] =i
		data["temperature"] = cel_temp
		data["humidity"] = data_humidity[i].replace("%","")
		data_dict.append(data)

	return data_dict

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=8009)