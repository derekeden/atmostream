from forecast import Forecast

if __name__ == '__main__':

    ################################################################################
    # HRDPS_continental
    ################################################################################

    # #define model
    # model = 'HRDPS_continental'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[0]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = ['WIND_AGL-10m','WDIR_AGL-10m','PRES_Sfc', 'PRMSL_MSL']
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()

    ################################################################################
    # HRDPS_north
    ################################################################################

    # #define model
    # model = 'HRDPS_north'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[0]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = ['UGRD_TGL_10','VGRD_TGL_10','PRES_SFC', 'PRMSL_MSL', 'WDIR_TGL_10','WIND_TGL_10']
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################
    # RDPS
    ################################################################################

    # #define model
    # model = 'RDPS'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[0]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = ['WIND_TGL_10','WDIR_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10','VGRD_TGL_10']
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################
    # GDPS
    ################################################################################

    # #define model
    # model = 'GDPS'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[0]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = ['WIND_TGL_10','WDIR_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10','VGRD_TGL_10']
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################
    # GEPS
    ################################################################################

    # #define model
    # model = 'GEPS'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[0]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = ['WIND_TGL_10','PRES_SFC', 'PRMSL_MSL', 'UGRD_TGL_10m','VGRD_TGL_10m']
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################
    # NAM_conusnest
    ################################################################################

    #define model
    model = 'NAM_conusnest'
    #init Forecast class
    fc = Forecast(model, output_path=f'{model}_test')
    #define start day/forecast
    day = fc.get_available_days()[-1]
    forecast = fc.get_available_forecasts(day)[0]
    #define variables to download
    vars = ['u10', 'v10', 'sp']
    #set and verify stream parameters
    fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    #stream data
    fc.stream()