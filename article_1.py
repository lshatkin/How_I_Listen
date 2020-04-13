import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import plotly.figure_factory as ff
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objects as go
from plotly.subplots import make_subplots
init_notebook_mode(connected=True)

def get_data(folder):
    data = pd.read_json("%s/StreamingHistory0.json"%folder, convert_dates = ['endTime'])
    data2 =  pd.read_json("%s/StreamingHistory1.json"%folder, convert_dates = ['endTime'])
    data = data.append(data2)
    data['count'] = 1
    data['total_listens'] = data.groupby('artistName')['count'].transform(pd.Series.cumsum)
    artists = data.groupby(['artistName']).count()
    relevant = artists.sort_values(by = 'count', ascending = False).iloc[0:10].index
    relevant_data = data.loc[data.artistName.isin(relevant)].sort_values(by = "total_listens")
    return data, relevant_data

def plot_total_listens(relevant_data):
    idx = relevant_data.groupby(['artistName'])['total_listens'].transform(max) == relevant_data['total_listens']
    relevant_sum = relevant_data[idx]
    return relevant_sum


def fill_time_period(df, dic, start, end):
    df = df.loc[df['endTime'] < end]
    df = df.loc[df['endTime'] > start]
    
    # Get the proportion of songs played by this artist compared
    # to total songs played in the time period
    artists = df.groupby(['artistName']).count()
    artists['proportion'] = artists['count'] / len(df.index)
    dic[end]["props"] = artists[['count','proportion']]
    
    # Get the total number of listens up to this point in time
    # (meaning listens from beginning of time until current date)
    idx = df.groupby(['artistName'])['total_listens'].transform(max) == df['total_listens']
    dic[end]["total_listens"] = df[idx][['artistName', 'total_listens']]
    

def get_time_periods_dfs(data, _weeks = 1):
    time_period_data = {}
    final_end_date = data.iloc[-1]['endTime']
    
    # Start input weeks into the future and look back
    # when gather data
    date = data.iloc[0]['endTime']  + timedelta(weeks = _weeks)
    while date < final_end_date:
        time_period_data[date] = {"props":None, "cumsum":None}
        start_date = date - timedelta(weeks = _weeks)
        fill_time_period(data, time_period_data, start_date, date)
        date += timedelta(weeks = _weeks)
    return time_period_data


def artist_level_data(data, time_period_data, base_prop = 0.01):
    import math
    artist_data = {}
    for artist in list(data.artistName.unique()):
        artist_data[artist] = {}
        first_date = True
        week_counter = 1
        
        for date, dic in time_period_data.items():
            artist_data[artist][date] = {}
            
            # Get the proportion of listens for this time frame
            props = dic["props"]
            artist_props = props[props.index == artist]
            prop = artist_props['proportion'][0] if len(artist_props.index) > 0 else base_prop
            artist_data[artist][date]["prop"] = prop
            
            # Get the current total listens up to this time frame
            total = dic["total_listens"]
            artist_sum = total[total.artistName == artist]
            if len(artist_sum.index) == 0:
                artist_data[artist][date]["sum"] = 0 if first_date else artist_data[artist][prev_date]["sum"]
            else:
                artist_data[artist][date]["sum"] = artist_sum['total_listens'].iloc[0]
            
            # Reset variables
            artist_data[artist][date]['week'] = week_counter
            prev_date = date
            week_counter += 1
            if first_date:
                first_date = False

    df = pd.DataFrame.from_dict({(i, j): artist_data[i][j]
            for i in artist_data.keys()
            for j in artist_data[i].keys()},
            orient = 'index')

    return df

def time_period_top_songs(data, _weeks = 1, n = 10):
    weekly = get_time_periods_dfs(data, _weeks)
    weekly_top = {}
    weekly_top_artist = {}
    for date, dic in weekly.items():
        props = dic['props'].sort_values(by = 'proportion', ascending = False)
        top = props.iloc[0:n]['proportion'].to_dict()
        weekly_top[date] = top.values()
        weekly_top_artist[date] = top.keys()

    weekly_top_df = pd.DataFrame.from_dict(weekly_top, 'index', columns = ["%d_song"%i for i in range(0, n)])
    song_df = pd.DataFrame(columns = ['prop', 'song'])
    for i in range(0, weekly_top_df.shape[1]):
        name = "%d_song"%i
        col = pd.DataFrame(weekly_top_df.iloc[:, i])
        col = col.rename(columns={name:"prop"})
        col = col.assign(artist_rank = "%d"%(i+1))
        song_df = song_df.append(col)

    weekly_top_df_artist = pd.DataFrame.from_dict(weekly_top_artist, 'index', columns = ["%d_artist"%i for i in range(0, n)])
    artist_df = pd.DataFrame(columns = ['artist'])
    for i in range(0, weekly_top_df_artist.shape[1]):
        name = "%d_artist"%i
        col = pd.DataFrame(weekly_top_df_artist.iloc[:, i])
        col = col.rename(columns={name:"artist"})
        artist_df = artist_df.append(col)

    result = pd.concat([song_df, artist_df], axis=1, sort=False)
    result['date'] = result.index.strftime("%m/%d/%Y")    
    return result

def weekly_half_prop_and_bands(data, name):
    time_period_data = get_time_periods_dfs(data)
    num_bands_for_half = {}
    for date, dics in time_period_data.items():
        props = dics['props'].sort_values(by = 'proportion', ascending = False)
        props['artist'] = props.index
        prop = 0
        band_count = 0
        bands = []
        while prop < 0.5:
            prop += props.iloc[band_count]['proportion']
            bands.append(props.iloc[band_count]['artist'])
            band_count += 1
        num_bands_for_half[date] = {}
        num_bands_for_half[date]['half_count'] = band_count
        num_bands_for_half[date]['num_bands'] = len(props.index)
        num_bands_for_half[date]['bands'] = bands
    half = pd.DataFrame(num_bands_for_half).T
    half['person'] = name
    return half