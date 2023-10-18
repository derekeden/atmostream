################################################################################

from atmostream import Forecast

################################################################################

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
    # vars = fc.supported_vars
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()

    ################################################################################
    # HRDPS_north
    ################################################################################

    #define model
    model = 'HRDPS_north'
    #init Forecast class
    fc = Forecast(model, output_path=f'{model}_test')
    #define start day/forecast
    day = fc.get_available_days()[0]
    forecast = fc.get_available_forecasts(day)[0]
    #define variables to download
    vars = fc.supported_vars
    #set and verify stream parameters
    fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    #stream data
    fc.stream()
    
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
    # vars = fc.supported_vars
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
    # vars = fc.supported_vars
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
    # vars = fc.supported_vars
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################
    # NAM_conusnest
    ################################################################################

    # #define model
    # model = 'NAM_conusnest'
    # #init Forecast class
    # fc = Forecast(model, output_path=f'{model}_test')
    # #define start day/forecast
    # day = fc.get_available_days()[-1]
    # forecast = fc.get_available_forecasts(day)[0]
    # #define variables to download
    # vars = fc.supported_vars
    # #set and verify stream parameters
    # fc.set_stream_params(day, forecast ,vars, 300, convert_to_dfs=True, auto_delete=False, logging=True)
    # #stream data
    # fc.stream()
    
    ################################################################################