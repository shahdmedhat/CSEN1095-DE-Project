from pandas.api.types import is_numeric_dtype

def handleOutliers(df):
    outliers=[]
    lowerUpper=[]
    for i in df:
        if (is_numeric_dtype(df[i])):
            Q1 = df[i+''].quantile(0.25)
            Q3 = df[i+''].quantile(0.75)
            IQR = Q3 - Q1
            cut_off = IQR * 1.5
            lower = Q1 - cut_off
            upper =  Q3 + cut_off
            df1 = df[df[i+'']> upper]
            df2 = df[df[i+''] < lower]
            if (df1.shape[0]+ df2.shape[0] >0):
                outliers.append(i)
                lowerUpper.append([i,lower,upper])
                
    for i in lowerUpper:
        if (i[0] != 'number_of_casualties'):
            df= df[(df[i[0]] < i[2]) & (df[i[0]] > i[1])]            