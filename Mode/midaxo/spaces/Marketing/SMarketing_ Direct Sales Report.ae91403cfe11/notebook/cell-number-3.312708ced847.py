raw = datasets['380e9e437dec']
sortorder = ['TOTAL','ENGAGED','ACTIVE_DEAL','QUALIFIED_DEAL','WON']
raw['MEASURE'] = pd.Categorical(raw['MEASURE'],sortorder)
df = raw.groupby(['MEASURE']).sum().reset_index()
df_filter = df['MEASURE'] != 'TOTAL'
df = df[df_filter]
