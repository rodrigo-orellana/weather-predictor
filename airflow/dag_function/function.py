from statsmodels.tsa.arima_model import ARIMA
import pandas as pd
import pmdarima as pm
import pymongo
import csv

def insert_to_mongo(ds, **kwargs):

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["weather"]
    my_collection = mydb["snfco"]
    result = my_collection.delete_many({})
    with open('/home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco_to_bd.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=';')
        for row in readCSV:       
            if(row[0]=='DATE'): #cabecera sin formato float           
                mydict = { 
                        "date" : row[0],
                        "temp" : row[1],
                        "hum"  : row[2] 
                        }
                #print(mydict)
            # x = my_collection.insert_one(mydict)  NO  ENVIAR cabecera a BD
            else: #datos no cabecera if(row[1]!='' and row[2]!=''):
                if(row[1].strip()!='' and row[2].strip()!=''):          
                    mydict = { 
                            "date" : row[0],
                            "temp" : float(row[1]),
                            "hum"  : float(row[2]) 
                            }
                    #print('datos')
                    #print(mydict)
                    x = my_collection.insert_one(mydict)
    myclient.close()


def delete_and_make_prediction(ds, **kwargs):
	myclient = pymongo.MongoClient("mongodb://localhost:27017/")
	db=myclient.weather
	collection = db.snfco
	data = collection.find()
	data_temperature = algorithmT(data, 72)
	data = collection.find()
	data_humidity = algorithmH(data, 72)
	#Creamos el diccionario
	data_dict = doDict(data_humidity, data_temperature)
	#abrimos la colección prediction
	my_collection = db["prediction"]
    #borramos su contenido
	result = my_collection.delete_many({})
    #insertamos nuevo contenido
	x = my_collection.insert_many(data_dict)


def doDict(data_humidity, data_temperature):
	data_dict = []
	for i in range(0, 72):
		data = {}
		data["hour"] =i
		data["temperature"] = data_temperature[i]
		data["humidity"] = data_humidity[i]
		data_dict.append(data)
	return data_dict

def algorithmH(collection, hours):
	
	df = pd.DataFrame(collection)
	# Entrenamos un subconjunto de los datos:
	df = df.sample(10000, random_state=38)
	#print(df["hum"].fillna(0))
	model = pm.auto_arima(df["hum"].fillna(0) , start_p=1, start_q=1,
		                  test='adf',  # use adftest to find optimal 'd'
		                  max_p=3, max_q=3,  # maximum p and q
		                  m=1,  # frequency of series
		                  d=None,  # let model determine 'd'
		                  seasonal=False,  # No Seasonality
		                  start_P=0,
		                  D=0,
		                  trace=True,
		                  error_action='ignore',
		                  suppress_warnings=True,
		                  stepwise=True)

    # Forecast
	n_periods = hours
	fc, confint = model.predict(n_periods=n_periods, return_conf_int=True)
	# fc contains the forecasting for the next 72 hours.	
	return fc

def algorithmT(collection, hours):

	df = pd.DataFrame(collection)
	# Entrenamos un subconjunto de los datos:
	df = df.sample(10000, random_state=38)
	#print(df["temp"].fillna(0))
	model = pm.auto_arima(df["temp"].fillna(0) , start_p=1, start_q=1,
		                  test='adf',  # use adftest to find optimal 'd'
		                  max_p=3, max_q=3,  # maximum p and q
		                  m=1,  # frequency of series
		                  d=None,  # let model determine 'd'
		                  seasonal=False,  # No Seasonality
		                  start_P=0,
		                  D=0,
		                  trace=True,
		                  error_action='ignore',
		                  suppress_warnings=True,
		                  stepwise=True)
	# Forecast
	n_periods = hours
	fc, confint = model.predict(n_periods=n_periods, return_conf_int=True)
	# fc contains the forecasting for the next 72 hours.	
	return fc