from forecast import Forecast

source = 'EC'
model = 'HRDPS_continental'
fc = Forecast(source, model)
fc.set_output_path('test')
day = fc.get_available_days()[0]
forecast = fc.get_available_forecasts(day)[0]
vars = ['WIND_AGL-10m','WDIR_AGL-10m','PRES_Sfc', 'PRMSL_MSL']
fc.set_stream_params(day, forecast ,vars, 30, convert_to_dfs=True, auto_delete=True)
fc.stream()