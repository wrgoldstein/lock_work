import pandas as pd
import time
nrows = None

t = time.time()
log = lambda s: print('[{} seconds]: {}'.format(round(time.time() - t, 2), s))
log('Started')

master = pd.read_csv('C:/Users/wilgol/Downloads/Compare/master_.csv', nrows=nrows, engine='python')
print(master.shape)
raise
log('Loaded master')
macp = pd.read_csv('C:/Users/wilgol/Downloads/Compare/macp_.csv', nrows=nrows, engine='python')
log('Loaded macp')
sar100 = pd.read_csv('C:/Users/wilgol/Downloads/Compare/sar100_.csv', nrows=nrows, engine='python')
log('Loaded sar100')

codes = pd.Series(master.columns).apply(lambda s: s.split(' ')[-1])
columns = pd.Series(master.columns).apply(lambda s: ' '.join(s.split(' ')[:-1]))
master.columns = columns
lookup = dict(zip(columns, codes))
log('Separated code and description')

m_master = pd.melt(master, id_vars=['Item Number'])
m_macp = pd.melt(macp, id_vars=['Item Number'])
m_sar100 = pd.melt(sar100, id_vars=['Item Number'])
log('Melted the separate data frames')

m_master_macp = pd.merge(m_master, m_macp, on=['Item Number', 'variable'], how='outer')
log('Merge round 1')
m_all = pd.merge(m_master_macp, m_sar100, on=['Item Number', 'variable'], how='outer')
log('Merge round 2')
m_all.to_csv('C:/Users/wilgol/Downloads/merged_partial.csv', index=False)
log('Wrote to csv (partial)')

m_all.columns = 'Item Number', 'Description', 'Master', 'MACP', 'SAR100'
m_all['Code'] = m_all['Description'].apply(lambda s: lookup.get(s))
m_all = m_all[['Item Number', 'Code', 'Description', 'Master', 'MACP', 'SAR100']]

m_all.to_csv('C:/Users/wilgol/Downloads/Compare/merged_.csv', index=False)

log('Done')
