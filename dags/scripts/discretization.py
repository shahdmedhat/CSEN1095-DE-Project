import datetime
import numpy as np
import pandas as pd

def get_week_number(date, year):
  week = 0
  end_date = datetime.datetime(year, 12, 31)
  delta = end_date - date
  if(delta != 0):
    week = 52 - delta.days
  return int(week)

def discretize(df):
  dates = df['date']
  weeks = np.zeros(len(dates))
  for i in range(len(dates)):
    if(pd.isnull(dates.iloc[i])):
      weeks[i] = 0
    else:
      weeks[i] = get_week_number(dates.iloc[i], df.iloc[i]['accident_year'])
  df['week_number'] =  weeks
