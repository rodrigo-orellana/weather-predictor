import unittest
from falcon import HTTP_400, HTTP_200
from scrapy_weather import app

class TestAPI(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    def test_24horas_api_v2(self):
        request = self.app.get('/servicio/v2/prediccion/24horas/')
        self.assertEqual(request.status, HTTP_200)
        self.assertTrue(len(request.json) == 24)
        self.assertTrue("hour" in request.json[0].keys())
        self.assertTrue("temperature" in request.json[0].keys())
        self.assertTrue("humidity" in request.json[0].keys())
    
    def test_48horas_api_v2(self):
        request = self.app.get('/servicio/v2/prediccion/48horas/')
        self.assertEqual(request.status, HTTP_200)
        self.assertTrue(len(request.json) == 48)
        self.assertTrue("hour" in request.json[0].keys())
        self.assertTrue("temperature" in request.json[0].keys())
        self.assertTrue("humidity" in request.json[0].keys())
    
    def test_72horas_appi_v2(self):
        request = self.app.get('/servicio/v2/prediccion/72horas/')
        self.assertEqual(request.status, HTTP_200)
        self.assertTrue(len(request.json) >= 71) #problema en la fuente
        self.assertTrue("hour" in request.json[0].keys())
        self.assertTrue("temperature" in request.json[0].keys())
        self.assertTrue("humidity" in request.json[0].keys())


if __name__ == "__main__":
    unittest.main() 
	