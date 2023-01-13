

def handleMissingValues(df):
    df['second_road_number']= df['second_road_number'].fillna('zero')
    df['road_type'] = df['road_type'].fillna(df['road_type'].mode()[0])
    df['weather_conditions'] = df['weather_conditions'].fillna(df['weather_conditions'].mode()[0])
