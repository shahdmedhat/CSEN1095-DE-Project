from sklearn import preprocessing

def labelEncoding(df):
    mappings = []

    columnsToEncode = []
    le = preprocessing.LabelEncoder()
    for columnName, columnValues in df.items():
        if (df.dtypes[columnName] == object and columnName != 'time'):
            df[columnName] = le.fit_transform(df[columnName])

            if (columnName != "accident_reference"): 
                le_name_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
                mappings.append(le_name_mapping)
