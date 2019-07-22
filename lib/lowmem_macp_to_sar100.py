
import csv
from collections import defaultdict

root = 'C:/Users/wilgol/Downloads/Compare Master MACP SAR100/'

items = defaultdict(dict)
item_numbers = set()

f_colcode = lambda s: s.split(' ')[-1]
f_colname = lambda s: ' '.join(s.split(' ')[:-1])

filename = root + '/master_.csv'
with open(filename, newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    i = 0
    for row in spamreader:
        if i == 0:
            header = list(map(f_colname, row))
            code = list(map(f_colcode, row))
            header_codes = dict(zip(header, code))
            j = header.index('Item Number')
            k = header.index('Stocking Type Code')
            i += 1
        else:
            if (row[k] == 'O'):
                print("Skipping", row[j])
                continue
            item_numbers.add(row[j])
            items[row[j]]['Master'] = dict(zip(header, row))


filename = root + '/macp_.csv'
with open(filename, newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    i = 0
    for row in spamreader:
        if i == 0:
            header = row
            j = header.index('Item Number')
            k = header.index('Stocking Type Code')
            i += 1
        else:
            if (row[k] == 'O'):
                print("Skipping", row[j])
                continue
            items[row[j]]['MACP'] = dict(zip(header, row))

filename = root + '/sar100_.csv'
with open(filename, newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    i = 0
    for row in spamreader:
        if i == 0:
            header = row
            j = header.index('Item Number')
            k = header.index('Stocking Type Code')
            i += 1
        else:
            if (row[k] == 'O'):
                print("Skipping", row[j])
                continue
            items[row[j]]['SAR100'] = dict(zip(header, row))

import pandas as pd

item_numbers = list(sorted(item_numbers))


def item_to_frame(item_number):
    item = items[item_number]
    item = dict(list(filter(lambda v: len(v[1]), item.items())))
    df = pd.DataFrame(item).reset_index()

    required_columns = ['Master', 'MACP', 'SAR100']

    columns = [c for c in required_columns if c in item]

    df.columns = ['Field'] + columns
    df['Item Number'] = item_number
    df['Code'] = [header_codes.get(i, 'No Match') for i in df['Field']]

    for column in required_columns:
        if column not in df.columns:
            df[column] = None

    df = df[['Item Number', 'Field', 'Code', 'Master', 'MACP', 'SAR100']]

    return df.fillna("<<DNE>>")


to_concat = []
i = 0
for item_number in item_numbers:
    print('.', end='')
    df = item_to_frame(item_number)
    to_concat.append(df)

    i += 1

    if i % 5000 == 0:
        print('\nWriting items...', end=' ')
        big = pd.concat(to_concat)
        big = big[big.Master != big.SAR100]
        form = {'item1': big['Item Number'].min(), 'item2': big['Item Number'].max()}
        filename = root + '/chunk_{item1}_to_{item2}.csv'.format(**form)
        big.to_csv(filename, index=False)
        to_concat = []
        print(' {} items written'.format(i))
        del big

