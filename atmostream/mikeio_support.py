import mikeio
from mikeio import Grid2D, EUMType, EUMUnit, ItemInfo
import glob
from datetime import datetime as dt
from datetime import timedelta as td
import re
import xarray as xr
import numpy as np
import pyproj
import pandas as pd
from tqdm import tqdm
import os
from more_itertools import sort_together

def var_mapper(var):
    mapper = {'WIND_AGL-10m':{'item':EUMType.Wind_speed,
                             'unit':EUMUnit.meter_per_sec},
              'WDIR_AGL-10m':{'item':EUMType.Wind_Direction,
                             'unit':EUMUnit.degree},
              'PRMSL_MSL':{'item':EUMType.Pressure,
                             'unit':EUMUnit.pascal},
              'PRES_Sfc':{'item':EUMType.Pressure,
                             'unit':EUMUnit.pascal},
              'WIND_Sfc':{'item':EUMType.Wind_speed,
                             'unit':EUMUnit.meter_per_sec},
              'WDIR_Sfc':{'item':EUMType.Wind_Direction,
                             'unit':EUMUnit.degree},
              'WSPD_Sfc':{'item':EUMType.Wind_speed,
                             'unit':EUMUnit.meter_per_sec},
              'pressfc':{'item':EUMType.Pressure,
                             'unit':EUMUnit.pascal},
              'sp':{'item':EUMType.Pressure,
                             'unit':EUMUnit.pascal},
              'wnd10m_u':{'item':EUMType.u_velocity_component,
                          'unit':EUMUnit.meter_per_sec},
              'wnd10m_v':{'item':EUMType.v_velocity_component,
                          'unit':EUMUnit.meter_per_sec},
              'u10':{'item':EUMType.u_velocity_component,
                          'unit':EUMUnit.meter_per_sec},
              'v10':{'item':EUMType.v_velocity_component,
                          'unit':EUMUnit.meter_per_sec},
              'tmpsfc':{'item':EUMType.Temperature,
                        'unit':EUMUnit.degree_Celsius}}
    return mapper[var]

def file_to_time(file, model):
    if 'HRDPS' in model:
        time = dt.strptime(re.search('\d{8}T\d{2}', file).group(), '%Y%m%dT%H')
        forecast_hour = td(hours=int(re.search('PT(\d+)H', file).group(1)))
        outtime = time+forecast_hour
    return outtime

def get_model_grid(model):
    grid = {'HRDPS_continental': {'ny': 1290, 
                           'nx': 2540, 
                           'orientation':0,
                           #'origin':(-14.82122, -12.302501), #from file
                           'origin':(-14.74122, -12.492501), #fits better
                           'dx': 0.0225,
                           'dy': 0.0225,
                           'projection':'PROJCS["EC_Conti",GEOGCS["Unused",DATUM["User defined",SPHEROID["Sphere (Radius = 6371229)",6371229,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433]],PROJECTION["Rotated_Longitude_Latitude"],PARAMETER["Longitude_Of_South_Pole",-114.694858],PARAMETER["Latitude_Of_South_Pole",-36.08852],PARAMETER["Angle_Of_Rotation",0],UNIT["Degree",1]]'},
            'CFS': {'ny': 190, 
                    'nx': 384, 
                    'orientation':0,
                   'x0': 0, 
                   'y0': -89.276712888,
                    'dx': 0.93749869,
                    'dy': 0.9447271204032363,
                    'projection':'LONG/LAT'},
            'NAM_conusnest': {'ny': 1059, 
                           'nx': 1799, 
                           'orientation':0.15,
                           'origin':(-2700339.06887162, -1597145.996848),
                           'dx': 3000.0,
                           'dy': 3000.0,
                        'projection':'PROJCS["NAM_conusnest",GEOGCS["Unused",DATUM["D_unnamed",SPHEROID["Sphere",6371229.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-97.5],PARAMETER["Standard_Parallel_1",38.5],PARAMETER["Standard_Parallel_2",38.5],PARAMETER["Latitude_Of_Origin",38.5],UNIT["Meter",1.0]]'}
            }
    return grid[model]

def to_dfs(folder, source, model, vars):
    if source == 'EC':
        if 'HRDPS' in model:
            HRDPS_to_dfs(folder, vars, model)
    elif source == 'NOAA':
        if 'cfs' == model.lower():
            CFS_to_dfs(folder, vars, model)
        if model.lower().startswith('nam'):
            NAM_to_dfs(folder, vars, model)

def remove_dfs(folder, source, model, vars):
    if source == 'EC':
        if 'HRDPS' in model:
            HRDPS_dfs_remove(folder, vars)
    elif source == 'NOAA':
        if 'cfs' == model.lower():
            CFS_dfs_remove(folder, vars)
            
def HRDPS_dfs_remove(folder, vars):
    var_files = [f for f in glob.glob(f'{folder}/*.grib2') if any([v in f for v in vars])]
    for f in var_files:
        os.remove(f)
    var_files = [f for f in glob.glob(f'{folder}/*.grib2*idx') if any([v in f for v in vars])]
    for f in var_files:
        os.remove(f)
     
def CFS_dfs_remove(folder, vars):
    var_files = [f for f in glob.glob(f'{folder}/*.grb2') if any([v in f for v in vars])]
    for f in var_files:
        os.remove(f)
    var_files = [f for f in glob.glob(f'{folder}/*.grb2*idx') if any([v in f for v in vars])]
    for f in var_files:
        os.remove(f)
            
def HRDPS_to_dfs(folder, vars, model):
    var_files = {var:[f for f in glob.glob(f'{folder}/*.grib2') if var in f] for var in vars}
    grid = Grid2D(**get_model_grid(model))
    for var in tqdm(vars, desc=f'{folder}...'):
        files = var_files[var]
        if len(files) == 0:
            continue
        time = [file_to_time(f, 'HRDPS') for f in files]
        typ, unit = var_mapper(var).values()
        data = []
        for file in tqdm(files, desc=var):
            f = xr.open_dataset(file)
            dat = f.variables[list(f.variables.keys())[-1]].values
            data.append(dat)
        data = np.array(data)
        data_array = mikeio.DataArray(data,
                                   geometry=grid,
                                   time=time,
                                   item=ItemInfo(var,
                                                 typ,
                                                 unit))
        dataset = mikeio.Dataset([data_array])
        dataset.to_dfs(f'{folder}/{var}.dfs2')
        
def CFS_to_dfs(folder, vars, model):
    var_files = {var:[f for f in glob.glob(f'{folder}/*.grb2') if var in f] for var in vars}
    grid = Grid2D(**get_model_grid(model))
    for var in tqdm(vars, desc=f'Converting files in {os.path.basename(folder)}...'):
        files = var_files[var]
        if len(files) == 0:
            continue
        else:
            file=files[0]
        if var == 'wnd10m':
            typ1, unit1 = var_mapper(var+'_u').values()
            typ2, unit2 = var_mapper(var+'_v').values()
            multi = True
        else:            
            typ, unit = var_mapper(var).values()
            multi = False
        f = xr.open_dataset(file)
        t0 = f.time.to_numpy()
        if multi:
            data1 = f.variables[list(f.variables.keys())[-2]].values[:,::-1,:]
            data2 = f.variables[list(f.variables.keys())[-1]].values[:,::-1,:]
            time = np.array([pd.to_datetime(str(t0))+td(hours=i) for i in range(data1.shape[0])])
            data_array = []
            for d,name,typ,unit in zip([data1, data2], ['u','v'], [typ1, typ2], [unit1, unit2]):
                data_array.append(mikeio.DataArray(d,
                                                    geometry=grid,
                                                    time=time,
                                                    item=ItemInfo(var+f'_{name}',
                                                                    typ,
                                                                    unit)))
        else:
            data = f.variables[list(f.variables.keys())[-1]].values[:,::-1,:] #flip because of dy being negative
            time = np.array([pd.to_datetime(str(t0))+td(hours=i) for i in range(data.shape[0])])
            data_array = [mikeio.DataArray(data,
                                        geometry=grid,
                                        time=time,
                                        item=ItemInfo(var,
                                                        typ,
                                                        unit))]
        dataset = mikeio.Dataset(data_array)
        dataset.to_dfs(f'{folder}/{var}.dfs2')

def NAM_to_dfs(folder, vars, model):
    grid = Grid2D(**get_model_grid(model))
    files = glob.glob(f'{folder}/nam.*{model.lower().split("_")[-1]}*.grib2')
    for file in tqdm(files, desc=f'Converting files in {os.path.basename(folder)}...'):
        vs = []
        dat = []
        if 'u10' in vars or 'v10' in vars:
            wind = xr.open_dataset(file, filter_by_keys={'typeOfLevel':'heightAboveGround', 'level':10})
            if 'u10' in vars:
                u = wind.variables['u10'].values
                vs.append('u10')
                dat.append(u)
            if 'v10' in vars:
                v = wind.variables['v10'].values
                vs.append('v10')
                dat.append(v)
        if 'sp' in vars:
            pres = xr.open_dataset(file, filter_by_keys={'typeOfLevel':'surface', 'stepType':'instant'})
            p = pres.variables['sp'].values
            vs.append('sp')
            dat.append(p)
        time = wind.time.values
        data_array = []
        for var,data in zip(vs,dat):
            typ, unit = var_mapper(var).values()
            data_array.append(mikeio.DataArray(data,
                                            geometry=grid,
                                            time=time,
                                            item=ItemInfo(var,
                                                            typ,
                                                            unit)))
        dataset = mikeio.Dataset(data_array)
        dataset.to_dfs(f'{folder}/{"_".join(vars)}.dfs2')

def sort_files_by_time(files):
    times = []
    for file in files:
        t0 = mikeio.read(file, time=[0]).time[0]
        times.append(t0)
    out_times, out_files = sort_together([times, files])
    return out_files

def get_time_matrix(files):
    times = []
    ids = []
    Fs = []
    for file in files:
        time = mikeio.read(file).time.tolist()
        idx = list(range(len(time)))
        times.extend(time)
        ids.extend(idx)
        Fs.extend([file]*len(time))
    return pd.DataFrame({'time':times, 'idx':ids, 'file':Fs}).sort_values(by='time')

def get_merged_data(matrix):
    ids = matrix.groupby(matrix['file'])[['idx']].agg(list)
    data = {}
    for file in ids.index:
        ds = mikeio.read(file, time=ids.loc[file].idx)
        data[file] = ds
    shape = ds.geometry.ny, ds.geometry.nx
    out = np.full((len(matrix),*shape), np.nan)
    for i in range(out.shape[0]):
        f = matrix.iloc[i]['file']
        f_id = matrix.iloc[i]['idx']
        f_ids = ids.loc[f].idx
        f_idx = f_ids.index(f_id)
        dat = data[f][0].values[f_idx]
        out[i,:,:] = dat
    return out
        
def concat_dfs(files, outfile):
    files = pd.Series(files)
    files = files.groupby(files.apply(lambda x: os.path.basename(x))).agg(list)
    files = files.apply(sort_files_by_time)
    for var in tqdm(files.index, desc='Concatenating files...'):
        v = var.replace('.dfs2','')
        fs = files.loc[var]
        matrix = get_time_matrix(fs)
        idx = matrix.groupby(matrix['time'])[matrix.columns].agg('last')
        data = get_merged_data(idx)
        typ, unit = var_mapper(v).values()
        data_array = [mikeio.DataArray(data,
                                       geometry=mikeio.read(fs[0]).geometry,
                                       time=idx['time'].tolist(),
                                       item=ItemInfo(v,
                                                     typ,
                                                     unit))]
        dataset = mikeio.Dataset(data_array)
        dataset.to_dfs(f'{var}_{outfile}')  
        
def combine_dfs(files, outfile):
    #read files
    ds = []
    for file in files:
        ds.append(mikeio.read(file))
    #read times
    time = []
    _time = []
    for d in ds:
        time.append(d.time)
        _time.extend(d.time)
    #find overlapping times
    series = pd.Series(_time)
    counts = series.value_counts()
    matching = sorted(counts[counts==len(ds)].index.tolist())
    ids = []
    for t in time:
        ids.append(np.searchsorted(t, matching))
    #read only overlapping data
    dat = []
    for i,d in enumerate(ds):
        dat.append(d.sel(time=ids[i])[0])
    #make output
    out = mikeio.Dataset(dat)
    out.to_dfs(outfile)