from sklearn.preprocessing import MinMaxScaler

def min_max_scaling(df, column):
  scaledData = MinMaxScaler().fit_transform(df[[column]])
  return scaledData

def normalize(df):
    df['longitude'] = min_max_scaling(df,"longitude")
    df['first_road_number'] = min_max_scaling(df,"first_road_number")
    df['second_road_number'] = min_max_scaling(df,"second_road_number")