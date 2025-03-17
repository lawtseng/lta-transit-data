import requests
import csv
import os
import datetime
from dateutil.relativedelta import relativedelta
import webbrowser


class LTADataMall:

    '''This class is used to set up the data source for LTA Data Mall API.
    
    Attributes:
        uri: the resource URL for LTA Data Mall API
        headers: the headers for the API request
    '''

    def __init__(self, account_key):
        self._uri = 'http://datamall2.mytransport.sg/ltaodataservice/'  # Resource URL
        self._headers = {
                        'AccountKey': account_key,
                        'accept': 'application/json'
                        }  
        
    @property
    def uri(self):
        return self._uri


class BusServiceData:

    '''This class is used to retrieve bus data from LTA Data Mall.
    
    Attributes:
        data_source: the data source for LTA Data Mall API
        type: the type of data to be retrieved
        url: the URL for the API request
    '''

    def __init__(self, data_source):
        self._data_source = data_source # lta data mall
        self._type = None 
        self._url = None

    @property
    def data_source(self):
        return self._data_source

    @property
    def type(self):
        return self._type

    def set_type(self, type: str):
        '''Set the type of data to be retrieved.'''
        if type == 'service':
            self._type = 'BusServices'
        elif type == 'stop':
            self._type = 'BusStops'
        elif type == 'route':
            self._type = 'BusRoutes'

    def set_url(self):
        '''Set the URL for the API request.'''
        return self._data_source.uri + self._type

    def get_service_data(self):
        '''Retrieve the bus service data from LTA Data Mall.'''
        self._url = self.set_url()
        self.results = []
        while True:
            new_results = requests.get(
                self._url,
                headers=self._data_source._headers,
                params={'$skip': len(self.results)}).json()['value']
            if new_results == []:
                break
            else:
                self.results += new_results
        return self.results
    
    def save_to_csv(self):
        '''Save the retrieved data to a CSV file.'''
        file_name = self.set_file_name()
        with open('saved_data/{}.csv'.format(file_name), 'w') as csvfile:
            field_names = self.set_field_name()
            writer = csv.DictWriter(csvfile, fieldnames = field_names)
            writer.writeheader()
            writer.writerows(self.results)

    def set_field_name(self):
        '''Set the field names for the CSV file.'''
        if self._type == 'BusServices':
            field_names = ['ServiceNo', 'Operator', 'Direction', 'Category', 'OriginCode', 'DestinationCode', 'AM_Peak_Freq', 
                            'AM_Offpeak_Freq', 'PM_Peak_Freq', 'PM_Offpeak_Freq', 'LoopDesc']
            return field_names
        elif self._type == 'BusStops':
            field_names = ['BusStopCode', 'Description', 'Latitude', 'Longitude', 'RoadName']
            return field_names
        elif self._type == 'BusRoutes':
            field_names = ['ServiceNo', 'Operator', 'Direction', 'StopSequence', 'BusStopCode', 
                            'Distance', 'WD_FirstBus', 'WD_LastBus', 'SAT_FirstBus', 'SAT_LastBus', 
                            'SUN_FirstBus', 'SUN_LastBus']
            return field_names
    
    def set_file_name(self):
        '''Set the file name for the CSV file.'''
        if self._type == 'BusServices':
            file_name = 'bus_services_' + self.set_time()
            return file_name
        elif self._type == 'BusStops':
            file_name = 'bus_stops_' + self.set_time()
            return file_name
        elif self._type == 'BusRoutes':
            file_name = 'bus_routes_' + self.set_time()
            return file_name

    def set_time(self):
        '''Set the time for the file name.'''
        month = datetime.date.today().month
        year = datetime.date.today().year
        y_m = str(year) + str(month).zfill(2)
        return y_m

class PassengerVolumeData:

    '''This class is used to retrieve public transit data from LTA Data Mall.
    
    Attributes:
        data_source: the data source for LTA Data Mall API
        type: the type of data to be retrieved
        url: the URL for the API request
    '''

    def __init__(self, data_source):
        self._data_source = data_source # lta data mall
        self._type = None 
        self._url = None

    @property
    def data_source(self):
        return self._data_source

    @property
    def type(self):
        return self._type

    def set_type(self, type: str):
        '''Set the type of data to be retrieved.'''
        if type == 'bus_node': # bus node volume
            self._type = 'PV/Bus'
        elif type == 'bus_od': # bus od volume
            self._type = 'PV/ODBus'
        elif type == 'train_node': # train node volume
            self._type = 'PV/Train'
        elif type == 'train_od': # train od volume
            self._type = 'PV/ODTrain'

    def set_url(self):
        '''Set the URL for the API request.'''
        return self.data_source.uri + self._type

    def get_pt_data(self, date=None):
        '''Retrieve the public transit data from LTA Data Mall.'''
        self._url = self.set_url()
        self.results = []
        params = {'$skip': len(self.results)}
        if date:
            params['Date'] = date
        self.results = requests.get(
            self._url, 
            headers=self.data_source._headers, 
            params=params).json()['value']
        webbrowser.open(self.results[0]['Link'])


class GeospatialData:
    
        def __init__(self, data_source):
            self._data_source = data_source # lta data mall
            self._type = None 
            self._url = None
    
        @property
        def data_source(self):
            return self._data_source
    
        @property
        def type(self):
            return self._type
    
        def set_type(self, type: str):
            if type == 'train_station': # bus node volume
                self._type = 'TrainStation'
    
        def set_url(self):
            return self.data_source.uri + self._type
    
        def get_geospatial_data(self):
            self._url = self.set_url()
            self.results = []
            self.results = requests.get(
                self._url, 
                headers=self.data_source._headers, 
                params={'$skip': len(self.results)}).json()['value']
            webbrowser.open(self.results[0]['Link'])

class DataCollector:

    '''This class is used to collect data from LTA Data Mall.'''

    

    def __init__(self, account_key):    
        self.data_source = LTADataMall(account_key)
        self._bus_data = BusServiceData(self.data_source)

    def set_time(self):
        '''Set the time for the file name as the previous month.'''
        today = datetime.date.today()
        previous_month = today - relativedelta(months=1)
        y_m = str(previous_month.year) + str(previous_month.month).zfill(2)
        return y_m

    def get_bus_data(self, type:str):
        '''Get bus data from LTA Data Mall.'''
        self._bus_data.set_type(type)
        self._bus_data.get_service_data()
        self._bus_data.save_to_csv()

    def get_pt_data(self, type:str, date=None):
        '''Get public transit passenger volume data from LTA Data Mall.'''
        self._pt_data = PassengerVolumeData(self.data_source)
        self._pt_data.set_type(type)
        self._pt_data.get_pt_data(date)

    def get_geospatial_data(self, type:str):
        '''Get geospatial data from LTA Data Mall.'''
        self._geospatial_data = GeospatialData(self.data_source)
        self._geospatial_data.set_type(type)
        self._geospatial_data.get_geospatial_data()

    def collect_transit_data(self):
        '''Collect transit data from LTA Data Mall.'''
        self.get_bus_data('service')
        self.get_bus_data('stop')
        self.get_bus_data('route')
        
        month = self.set_time()
        self.get_pt_data('bus_node', date = month)
        self.get_pt_data('bus_od', date = month)
        self.get_pt_data('train_node', date = month)
        self.get_pt_data('train_od', date = month)







