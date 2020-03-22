# Helpers for loading and transforming the COVID-19 data provided the John Hopkins University

import pandas as pd

def get_jhu_data(
        url_prefix = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/',
        confirmed_file = 'time_series_19-covid-Confirmed.csv',
        recovered_file = 'time_series_19-covid-Recovered.csv',
        deaths_file = 'time_series_19-covid-Deaths.csv'
    ):
    "Return confirmed, recovered, deaths according to https://github.com/CSSEGISandData/COVID-19"
    
    confirmed = pd.read_csv(url_prefix + confirmed_file)
    recovered = pd.read_csv(url_prefix + recovered_file)
    deaths    = pd.read_csv(url_prefix + deaths_file)
    
    return confirmed, recovered, deaths


def aggregte_jhu_by_state(confirmed, recovered, deaths):
    "Aggregate and reshape data from get_jhu to conveniently analyse cases by state"
    
    confirmed = confirmed.drop(['Province/State','Lat','Long'], axis=1).groupby('Country/Region').sum().T
    confirmed.index = pd.DatetimeIndex(confirmed.index, name='Date')
    
    recovered = recovered.drop(['Province/State','Lat','Long'], axis=1).groupby('Country/Region').sum().T
    recovered.index = pd.DatetimeIndex(recovered.index, name='Date')
    
    deaths = deaths.drop(['Province/State','Lat','Long'], axis=1).groupby('Country/Region').sum().T
    deaths.index = pd.DatetimeIndex(deaths.index, name='Date')
    
    infected = (confirmed - recovered - deaths)
    infection_rate = (infected / infected.shift(1))
    
    return pd.concat({
        'confirmed': confirmed, 'recovered': recovered, 'deaths': deaths, 'infected': infected, 'infection_rate': infection_rate
    }, axis=1)


def get_aggregate_top_n(jhu_data, metric='confirmed', n_states=20, n_rows=5):
    "Return at the most recent numbers for the states with the most cases."
    
    return jhu_data.iloc[-n_rows:,jhu_data.iloc[-1].argsort()[:-n_states:-1]]


def join_jhu_df(confirmed, recovered, deaths):
    "Return single DataFrame with JHU data and a list of columns names containing the counts for the different days"
    
    # get into shape
    non_date_cols = ['Country/Region', 'Province/State', 'Lat', 'Long']
    cols          = [pd.to_datetime(c).date() if c not in non_date_cols else c for c in confirmed.columns ]
    days          = [c for c in cols if not c in non_date_cols]
    
    confirmed = confirmed.set_axis(cols, axis=1, inplace=False).set_index(['Country/Region','Province/State'])
    recovered = recovered.set_axis(cols, axis=1, inplace=False).set_index(['Country/Region','Province/State'])
    deaths    = deaths.set_axis(cols, axis=1, inplace=False).set_index(['Country/Region','Province/State'])

    # calculate infected
    infected = confirmed.copy()
    infected.loc[:,days] -= recovered[days] + deaths[days]
    
    # combine
    return pd.concat({
        'confirmed': confirmed, 'recovered': recovered, 'deaths': deaths, 'infected': infected
    }, axis=1), days

