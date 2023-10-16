from forecast import Forecast

source = 'NOAA'
model = 'NAM_conusnest'
fc = Forecast(source, model)
fc.set_output_path('VancouverPortProject')
day = fc.get_available_days()[0]
forecast = fc.get_available_forecasts(day)[0]
vars = ['u10', 'v10', 'sp']
fc.set_stream_params(day, forecast ,vars, 30, convert_to_dfs=True, auto_delete=False)
fc.stream()

#FIX DOWNLOADER
#ADD LOGGING