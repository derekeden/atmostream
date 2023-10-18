import os
from datetime import datetime as dt
import requests
from bs4 import BeautifulSoup as bs
import re
import glob
from tqdm import tqdm
from time import sleep
import pandas as pd
from mikeio_support import to_dfs, remove_dfs

supported_models = ['HRDPS_continental',
                    'HRDPS_north',
                    'RDPS',
                    'GDPS',
                    'GEPS',
                    'HRRR_conus',
                    'HRRR_alaska',
                    'GFS_0p25',
                    'GFS_0p50',
                    'GFS_1p00',
                    'NAM_bgrdsf',
                    'NAM_conusnest',
                    'NAM_alaskanest',
                    'NAM_hawaiinest',
                    'NAM_prinest',
                    'CFS']

def source_mapper(model):
    mapper = {'HRDPS_continental': 'EC',
            'HRDPS_north': 'EC',
            'RDPS': 'EC',
            'GDPS': 'EC',
            'GEPS': 'EC',
            'HRRR_conus': 'NOAA',
            'HRRR_alaska': 'NOAA',
            'GFS_0p25':'NOAA',
            'GFS_0p50':'NOAA',
            'GFS_1p00':'NOAA',
            'NAM_bgrdsf': 'NOAA',
            'NAM_conusnest': 'NOAA',
            'NAM_alaskanest': 'NOAA',
            'NAM_hawaiinest': 'NOAA',
            'NAM_prinest': 'NOAA',
            'CFS': 'NOAA'}
    return mapper[model]

class Forecast:
    ############################################################################
    ############################################################################
    
    def __init__(self, model, output_path=None):
        assert model in supported_models, f"Choose a model from: {supported_models}"
        self.model = model
        self.source = source_mapper(model)
        self.set_output_path(output_path)
        
    ############################################################################
    ############################################################################
    
    @property
    def status(self):
        p = requests.get(self.data_url)
        return p.status_code
    
    @property
    def meta_url(self):
        return self._get_meta_url()
    
    @property
    def data_url(self):
        return self._get_data_url()
    
    @property
    def output_path(self):
        return self._output_path
    
    @property 
    def download_params(self):
        return self._download_params
    
    @property 
    def stream_params(self):
        return self._stream_params
    
    ############################################################################
    ############################################################################
    
    def set_output_path(self, pth):
        if pth is not None:
            self._output_path = os.path.abspath(pth)
        else:
            self._output_path = os.path.abspath('.')
        if not os.path.isdir(self.output_path):
            os.mkdir(self.output_path)
    
    def get_available_days(self, reset=True):
        if reset:
            if self.source == 'EC':
                days = [dt.utcnow().strftime('%Y%m%d')]
            else:
                days = self._get_available_days()
            self._available_days = days
        else:
            pass
        return self._available_days
    
    def get_available_forecasts(self, date):
        url = self._get_forecast_url(date)
        forecasts = self._get_available_forecasts(url)
        if self.source == 'EC':
            utchour = dt.utcnow().hour
            fcs = [f for f in forecasts if int(f) <= utchour]
            return fcs
        else:
            dat = pd.to_datetime(date).strftime('%Y%m%d')
            today = pd.Timestamp.today().strftime('%Y%m%d')
            if dat == today:
                utchour = dt.utcnow().hour
                fcs = [f for f in forecasts if int(f) <= utchour]
                return fcs
            else:
                return forecasts

    def get_available_files(self, date, forecast):
        if self.source == 'EC':
            links = []
            url = self._get_forecast_url(date) + f'/{forecast}/'
            page = requests.get(url) 
            soup = bs(page.text, features="html.parser")
            matches = soup.find_all('a')
            for match in matches:
                h = match.get('href')
                hh = re.search('^(\d+)/', h)
                if hh is not None:
                    hurl = f"{url}{hh.group(1)}/"
                    page = requests.get(hurl)
                    soup = bs(page.text, features="html.parser")
                    matches = soup.find_all('a')
                    _links = [f"{hurl}/{m.get('href')}" for m in matches if '.grib2' in m.get('href')]
                    links.extend(_links)
        elif self.source == 'NOAA':
            links = []
            mod =  self.model.split('_')[0].lower()
            if 'gfs' in self.model.lower():
                url = self._get_forecast_url(date) + f'{forecast}/atmos/'
                pattern = f'^{mod}.t{forecast}z.'
            elif 'cfs' in self.model.lower():
                url = self._get_forecast_url(date) + f'/{date+forecast}'
                pattern = f'.grb2'
            else:
                url = self._get_forecast_url(date)
                pattern = f'^{mod}.t{forecast}z.'
            page = requests.get(url) 
            soup = bs(page.text, features="html.parser")
            matches = soup.find_all('a')
            for match in matches:
                h = match.get('href')
                if re.search(pattern, h):
                    links.append(f"{url}/{h}")
        else:
            assert False, f'Model source "{self.source}" not supported...'
        return links
    
    def set_download_params(self, date, forecast, variables, verify=True):
        self._download_params = {'date':date,
                                 'forecast': forecast,
                                 'variables': variables}
        out_fld = self.output_path + f'/{self.source}-{self.model}-{date}-{forecast}'
        if not os.path.isdir(out_fld):
            os.mkdir(out_fld)
        self._download_path = out_fld
        if verify:
            self._verify_download_params()
        return 0       
    
    def get_download_files(self, check_output_path=False):
        p = self.download_params
        avail_files = self.get_available_files(p['date'], p['forecast'])
        out_files = self._filter_files_by_vars(avail_files, p['variables'])
        # #
        # #
        # #hack for now
        out_files = [l for l in out_files if l.lower().endswith('.grib2')]
        out_files = out_files[:1]
        # print(out_files)
        # #
        # #
        # #
        if check_output_path:
            exists = glob.glob(self._download_path+'/*') 
            exists = [os.path.basename(e) for e in exists if os.path.isfile(e)]
            out_files = [o for o in out_files if os.path.basename(o) not in exists]            
        return out_files
    
    def set_stream_params(self, startdate, startforecast, variables, sleep, verify=True, convert_to_dfs=False,
                          auto_delete=True, logging=True):
        self._stream_params = {'startdate': startdate,
                               'startforecast': startforecast,
                               'variables': variables,
                               'sleep': sleep,
                               'convert': convert_to_dfs,
                               'delete': auto_delete,
                               'logging': logging,
                               'verify': verify}
        if verify:
            self._verify_stream_params()
    
    def download(self, check_output_path=False):
        links = self.get_download_files(check_output_path=check_output_path)
        for link in tqdm(links, desc=f'Downloading files...'):
            self._download_file(link, self._download_path)
    
    def stream(self):
        #global catch all for now
        try:
            #initially set to starting stream params
            p = self.stream_params
            day = p['startdate']
            forecast = p['startforecast']
            vars = p['variables']
            convert = p['convert']
            delete = p['delete']
            logging = p['logging']
            verify = p['verify']
            #logging
            log_file = f'{self.output_path}/{self.source}_{self.model}_{dt.utcnow().isoformat().replace(":","-")}.log'
            if logging:
                f = open(log_file, 'w')
                f.write('STARTING PARAMETERS:\n')
                f.write(day+'\n')
                f.write(forecast+'\n')
                f.write(str(vars)+'\n')
                f.write(str(convert)+'\n')
                f.write(str(delete)+'\n')
                f.flush()
            while True:
                print(f'{self.model} download parameters - date : {day}, forecast : {forecast}, vars : {vars}')
                #reset switches
                nextfc = False
                nextday = False
                #start timer
                t1 = dt.now()
                if logging:
                    f.write(f"\n{t1}\n")
                    f.write(f'{self.model} download parameters - date : {day}, forecast : {forecast}, vars : {vars}\n')
                    f.flush()
                #set the download parameters
                self.set_download_params(day, forecast, vars, verify=verify)
                #get the new files to download based on current parameters
                files = self.get_download_files(check_output_path=True)
                if logging:
                    f.write(f"Available Files to Download:\n")
                    for file in files:
                        f.write(file+'\n')
                    f.flush()
                #check which forecast we're on
                allfc = self.get_available_forecasts(day)
                if logging:
                    f.write(f"Available Forecasts on this day:\n")
                    for fc in allfc:
                        f.write(fc+'\n')
                #what days are even available
                alldays = self.get_available_days(reset=False)
                if logging:
                    f.write(f"Available Days:\n")
                    for d in alldays:
                        f.write(d+'\n')
                    f.flush()
                #if no files, next fc... if files, download them
                if len(files) == 0:
                    #if its the latest forecast, might not be downloaded yet
                    if day==alldays[-1] and forecast==allfc[-1]:
                        if logging:
                            f.write('Monitoring latest forecast, but no data. Waiting...\n')
                        nextfc = False
                    else:
                        if logging:
                            f.write('Already fetched all files for this forecast...\n')
                            f.flush()
                        nextfc = True
                        if convert:
                            if logging:
                                f.write(f"Converting this forecast into DFS format...\n")
                                f.flush()
                            to_dfs(self._download_path, self.source, self.model, self.download_params['variables'])
                            if delete:
                                if logging:
                                    f.write("Removing original raw files...\n")
                                    f.flush()
                                remove_dfs(self._download_path, self.source, self.model, self.download_params['variables'])
                else:
                    if logging:
                        f.write('Downloading files...\n')
                        f.flush()
                    nextfc = False
                    self.download(check_output_path=True)
                #do we need to switch days
                if forecast == allfc[-1] and nextfc:
                    if logging:
                        f.write('Already on last forecast, switching to next day...\n')
                        f.flush()
                    nextday = True
                else:
                    if logging:
                        f.write('Still more forecasts or more data in current forecast, staying on this day...\n')
                        f.flush()
                    nextday = False                
                #can we switch days
                if day == alldays[-1]:
                    canswitch = False
                    if logging:
                        f.write('Already on last day, cannot switch to next day yet...\n')
                        f.flush()
                else:
                    canswitch = True
                    if logging:
                        f.write('Not on last day, can switch to next day...\n')
                        f.flush()
                #EC specific
                if self.source == 'EC':
                    if ((dt.utcnow().hour < int(forecast))):
                        canswitch = True
                        nextfc = True
                        nextday = True
                        alldays = self.get_available_days()
                        if logging:
                            f.write('EC model - UTC day has switched over. Switching to next day to avoid downloading duplicated data...\n')
                            f.write(f"Available Days:\n")
                            for d in alldays:
                                f.write(d+'\n')
                            f.flush()
                else:
                    pass
                #decide what the next download is
                if nextfc and not nextday:
                    if logging:
                        f.write('NEXT STEP: next forecast on the current day...\n')
                        f.flush()
                    forecast = allfc[allfc.index(forecast)+1]
                elif nextfc and nextday:
                    if logging:
                        f.write('Need to switch to next forecast and day...\n')
                        f.flush()
                    if canswitch:
                        if logging:
                            f.write('NEXT STEP: can switch, next forecast on the next day...\n')
                            f.flush()
                        if self.source == 'EC':
                            day = alldays[-1]
                        else:
                            day = alldays[alldays.index(day)+1]
                        newfcs = self.get_available_forecasts(day)
                        forecast = newfcs[0]
                    else:
                        if logging:
                            f.write('NEXT STEP: no new day of data yet...\n')
                            f.flush()
                        pass
                elif not nextfc and not nextday:
                    if logging:
                        f.write('NEXT STEP: check current forecast again for more data...\n')
                        f.flush()
                    pass
                t2 = dt.now()
                delta = (t2-t1).total_seconds()
                if logging:
                    f.write(f"{t2} took {delta} sec\n")
                    f.flush()
                if delta < p['sleep']:
                    wait = p['sleep'] - delta
                    if logging:
                        f.write('Waiting...\n')
                        f.flush()
                    sleep(wait)
        #if something goes wrong, assume reinitializing on latest forecast date
        except Exception as err:       
            if logging:
                f.write(repr(err)+'\n')
                f.flush()
            #check if last download settings still valid
            alldays = self.get_available_days()
            if day in alldays:
                newday = day
                allfcs = self.get_available_forecasts(day)
                if forecast in allfcs:
                    newfc = forecast
                else:
                    newfc = allfcs[-1]
            else:
                newday = alldays[0]
                newfc = self.get_available_forecasts(newday)[0]
            #reset streaming parameters
            if logging:
                f.write(f'RESETTING STREAM PARAMETERS TO {newday} {newfc}\n')
                f.flush()
                f.close()  
            self.set_stream_params(newday, 
                                   newfc,
                                   self.stream_params['variables'], 
                                   self.stream_params['sleep'],
                                   convert_to_dfs=self.stream_params['convert'],
                                   auto_delete=self.stream_params['delete'],
                                   logging=self.stream_params['logging'])
            #restart streaming
            self.stream()
                     
    ############################################################################
    ############################################################################
    
    #get model website
    def _get_meta_url(self):
        url = {'HRDPS_continental': 'https://eccc-msc.github.io/open-data/msc-data/nwp_hrdps/readme_hrdps-datamart_en/#high-resolution-deterministic-prediction-system-hrdps-data-in-grib2-format',
               'HRDPS_north': 'https://eccc-msc.github.io/open-data/msc-data/nwp_hrdps/readme_hrdps-datamart_en/#high-resolution-deterministic-prediction-system-hrdps-data-in-grib2-format',
               'RDPS': 'https://eccc-msc.github.io/open-data/msc-data/nwp_rdps/readme_rdps-datamart_en/',
               'GDPS': 'https://eccc-msc.github.io/open-data/msc-data/nwp_gdps/readme_gdps-datamart_en/',
               'GEPS': 'https://eccc-msc.github.io/open-data/msc-data/nwp_geps/readme_geps-datamart_en/',
               'HRRR_conus': 'https://www.nco.ncep.noaa.gov/pmb/products/hrrr',
               'HRRR_alaska': 'https://www.nco.ncep.noaa.gov/pmb/products/hrrr',
               'GFS_0p25':'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php',
               'GFS_0p50':'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php',
               'GFS_1p00':'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php',
               'NAM_bgrdsf': 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php',
               'NAM_conusnest': 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php',
               'NAM_alaskanest': 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php',
               'NAM_hawaiinest': 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php',
               'NAM_prinest': 'https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php',
               'CFS': 'https://www.ncei.noaa.gov/products/weather-climate-models/climate-forecast-system'}       
        return url[self.model]
    
    #get model data url
    def _get_data_url(self):
        url = {'HRDPS_continental': 'https://dd.weather.gc.ca/model_hrdps/continental/2.5km',
               'HRDPS_north': 'https://dd.weather.gc.ca/model_hrdps/north/grib2',
               'RDPS': 'https://dd.weather.gc.ca/model_gem_regional/10km/grib2',
               'GDPS': 'https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon',
               'GEPS': 'https://dd.weather.gc.ca/ensemble/geps/grib2/raw',
               'HRRR_conus': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod',
               'HRRR_alaska': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod',
               'GFS_0p25':'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod',
               'GFS_0p50':'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod',
               'GFS_1p00':'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod',
               'NAM_bgrdsf': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod',
               'NAM_conusnest': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod',
               'NAM_alaskanest': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod',
               'NAM_hawaiinest': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod',
               'NAM_prinest': 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod',
               'CFS': 'https://www.ncei.noaa.gov/data/climate-forecast-system/access/operational-9-month-forecast/time-series'}
        return url[self.model]
    
    def _get_supported_vars(self):
        vrs = {'HRDPS_continental': ['WIND_AGL-10m','WDIR_AGL-10m','PRES_Sfc', 'PRMSL_MSL', 'UGRD_AGL-10m','VGRD_AGL-10m'],
               'HRDPS_north': ['UGRD_TGL_10','VGRD_TGL_10','PRES_SFC', 'PRMSL_MSL', 'WDIR_TGL_10','WIND_TGL_10'],
               'RDPS': ['WIND_TGL_10','WDIR_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10','VGRD_TGL_10'],
               'GDPS': ['WIND_TGL_10','WDIR_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10','VGRD_TGL_10'],
               'GEPS': ['WIND_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10m','VGRD_TGL_10m'],
               'HRRR_conus': ['wrfsfc'],
               'HRRR_alaska': ['wrfsfc'],
               'GFS_0p25': ['pgrb2','pgrb2b','pgrb2full'],
               'GFS_0p50': ['pgrb2','pgrb2b','pgrb2full'],
               'GFS_1p00': ['pgrb2','pgrb2b','pgrb2full'],
               'NAM_bgrdsf': ['alaskanest','conusnest','prinest','hawaiinest', 'bgrdsf'],
               'NAM_conusnest': ['u10', 'v10', 'sp'],
               'NAM_alaskanest': ['alaskanest','conusnest','prinest','hawaiinest', 'bgrdsf'],
               'NAM_hawaiinest': ['alaskanest','conusnest','prinest','hawaiinest', 'bgrdsf'],
               'NAM_prinest': ['alaskanest','conusnest','prinest','hawaiinest', 'bgrdsf'],
               'CFS': ['pressfc', 'wnd10m', 'tmpsfc']}
        return vrs[self.model]
    
    def _get_available_days(self, nowcast=False):
        if self.source in ['NOAA']:
            mod = self.model.split('_')[0].lower()
            p = requests.get(self.data_url)
            soup = bs(p.text, features="html.parser")
            matches = soup.find_all('a')
            dates = []
            for match in matches:
                if mod == 'cfs':
                    t = match.get('href')
                    yr = re.search(f'(\d+)/', t)
                    if yr is not None:
                        year = yr.group(1)
                        #this will skip reading all the years for nowcast
                        if nowcast and int(year) != dt.utcnow().year:
                            pass
                        else:
                            print(f'Fetching available dates from {year}...')
                            year_url = self.data_url + f'/{year}'
                            yp = requests.get(year_url)
                            ysoup = bs(yp.text, features="html.parser")
                            ymatches = ysoup.find_all('a')
                            for ymatch in ymatches:
                                t = ymatch.get('href')
                                mo = re.search(f'(\d+)/', t)
                                if mo is not None:
                                    month = mo.group(1)
                                    month_url = year_url + f'/{month}'
                                    mp = requests.get(month_url)
                                    msoup = bs(mp.text, features="html.parser")
                                    mmatches = msoup.find_all('a')
                                    for mmatch in mmatches:
                                        t = mmatch.get('href')
                                        dy = re.search(f'(\d+)/', t)
                                        if dy is not None:
                                            day = dy.group(1)
                                            dates.append(day)
                else:
                    t = match.get('href')
                    fc = re.search(f'{mod}.(\d+)/', t)
                    if fc is not None:
                        dates.append(fc.group(1))
        else:
            assert False, f'Model source "{self.source}" not supported...'
        return dates
    
    def _get_forecast_url(self, date):
        if self.source == 'EC':
            url = self.data_url
        elif self.source == 'NOAA':
            mod = self.model.split('_')[0].lower()
            if mod == 'cfs':
                url = f'{self.data_url}/{date[:4]}/{date[:6]}/{date[:8]}'
            else:
                url = self.data_url + f"/{mod}.{date}/"
            if mod == 'hrrr':
                grid = self.model.split('_')[1].lower() 
                url = f'{url}/{grid}/'
        else:
            assert False, f'Model source "{self.source}" not supported...'
        return url    

    def _get_available_forecasts(self, url):
        if self.source == 'EC':
            lookup = '^(\d+)/'
        elif self.source == 'NOAA':
            if 'hrrr' in self.model.lower():
                lookup = '.t(\d{2})z.'
            elif 'gfs' in self.model.lower():
                lookup = r'(\d{2})/'
            elif 'nam' in self.model.lower():
                lookup = '.t(\d{2})z.'
            elif self.model.lower() == 'cfs':
                lookup = '\d{8}(\d{2})/'
        else:
            assert False, f'Model source "{self.source}" not supported...'
        p = requests.get(url)
        soup = bs(p.text, features="html.parser")
        matches = soup.find_all('a')
        forecasts = []
        for match in matches:
            t = match.get('href')
            fc = re.search(lookup, t)
            if fc is not None:
                forecasts.append(fc.group(1))
        return sorted(list(set(forecasts)))
                
    def _verify_download_params(self):
        p = self.download_params
        #check date available
        dates = self.get_available_days()
        msg = f'{p["date"]} not in available dates: {dates}'
        assert p['date'] in dates, msg
        #check forecast available
        forecasts = self.get_available_forecasts(p['date'])
        msg = f'{p["forecast"]} not in available forecasts: {forecasts}'
        # assert p['forecast'] in forecasts, msg
        #check vars supported
        vars = self._get_supported_vars()
        check = all([v in vars for v in p['variables']])
        msg = f'Not all requeste variables found available variables: {vars}'
        assert check, msg
        return 0 
    
    def _verify_stream_params(self):
        # print('Verfiying the stream parameters are valid before commencing...')
        p = self.stream_params
        #check date available
        dates = self.get_available_days()
        msg = f'{p["startdate"]} not in available dates: {dates}'
        assert p['startdate'] in dates, msg
        #check forecast available
        forecasts = self.get_available_forecasts(p['startdate'])
        msg = f'{p["startforecast"]} not in available forecasts: {forecasts}'
        # assert p['startforecast'] in forecasts, msg
        #check vars supported
        vars = self._get_supported_vars()
        check = all([v in vars for v in p['variables']])
        msg = f'Not all requeste variables found available variables: {vars}'
        assert check, msg
        return 0 
    
    def _filter_files_by_vars(self, files, vars):
        if self.source == 'EC':
            links = [l for l in files if any([f'_{kv}_' in l for kv in vars])]
        elif self.source == 'NOAA':
            if 'hrrr' in self.model.lower():
                links = [l for l in files if any([f'z.{kv}' in l for kv in vars])]
            elif 'gfs' in self.model.lower():
                res = self.model.lower().split('_')[1]
                links = [l for l in files if any([f'.{kv}.{res}' in l for kv in vars])]
            elif self.model.lower().startswith('nam'):
                    links = [l for l in files if f".{self.model.lower().split('_')[-1]}." in l]            
            elif 'cfs' == self.model.lower():
                links = [l for l in files if any([f'{kv}.' in l for kv in vars])]
        else:
            assert False, f'Model source "{self.source}" not supported...'
        return links
    
    def _download_file(self, link, out_pth):
        if not os.path.isdir(out_pth):
            os.mkdir(out_pth)
        filename = os.path.basename(link)
        dl = requests.get(link)
        with open(f'{out_pth}/{filename}', 'wb') as f:
            f.write(dl.content)
            
    ############################################################################
    ############################################################################
    
    