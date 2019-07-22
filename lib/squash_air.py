 #!/usr/bin/python
 # -*- coding: utf-8 -*-

import sys
import pandas as pd
import numpy as np
from glob import glob

import warnings
warnings.filterwarnings('ignore')

class RuleSquasher:
    
    def __init__(self, filename, output_dir):
        self.filename = filename
        self.name = filename.split('\\')[-1][:-5]
        print("[INFO] Initializing {name}".format(name=self.name))
        self.output_filename = output_dir + self.name + ".xlsx"
        
    def load_data(self):
        print("[INFO] Loading data for {name}".format(name=self.name))
        self.rules = pd.read_excel(self.filename, 'Rules', dtype={'Item Number': str})
        self.values = pd.read_excel(self.filename, 'Values', dtype={'Item Number': str})
        
        self.columns = ['Item Number', 'Rule #', 'L T', 'SEQ #', 'And /Or','Parent Sgmt','Def. Rel.',
               'Select Value', '2nd Item Number', 'Table Name',  'Derived Calculation', 'Ext Program ID']
        rules_subset = self.rules[self.columns]

        if len(self.values):
            # Some sheets have no values used
            
            self.values_columns = ['Item Number', 'Rule #', 'L T', 'SEQ #', 'Parent Sgmt', 'Segment Value']

            self.used_columns = self.columns[2:] + self.values_columns[4:]
            values_subset = self.values[self.values_columns]
            merged_subset = pd.merge(rules_subset, values_subset, 
                                     on=['Item Number', 'Rule #', 'L T', 'SEQ #', 'Parent Sgmt'], 
                                     how='outer')

            self.dataset = merged_subset
        else:
            self.used_columns = self.columns[2:]
            self.dataset = rules_subset

        print("[INFO] Loading complete")
        self.grouped = self.dataset.groupby(['Item Number', 'Rule #', 'L T']).apply(self.combine_rule)
        return self

    def combine_rule(self, group):
        "Take a multi rule `rule` and turn it into a single string"
        return '|'.join([';'.join([str(s) if s or s==0 else '' for s  in row]) for row in group.values[:,2:]])
    
    def get_min_row_and_part(self, group):
        row = group['index'].min()
        idx = group['index'].idxmin()
        item_number = group.loc[idx]['Item Number']
        return row, item_number
        
    def hydrate_rule(self, rule):
        "Take a rule created by `combine_rule` and expand it to multiple rows"
        values = [row.split(';') for row in rule.split('|')]
        return dict(zip(self.used_columns, np.transpose(values)))
    
    def rule_counts(self):
        rule_counts = self.grouped.value_counts().to_frame().reset_index().reset_index()
        rule_counts.columns = 'rule_id', 'rule', 'count'
        rule_counts.set_index('rule', inplace=True)
        return rule_counts
    
    def rule_order(self):
        return self.grouped.reset_index()\
                .reset_index()\
                .rename({0: 'rule'}, axis=1)\
                .groupby('rule')\
                .apply(self.get_min_row_and_part)\
                .apply(pd.Series)\
                .rename({0: "rule_order", 1: "Part"}, axis=1)
    
    def rule_parts(self):
        merged = pd.merge(self.grouped.to_frame('rule').reset_index(), 
                          self.rule_counts(), 
                          left_on='rule',
                          right_index=True).sort_values('count', ascending=False)
        return merged.groupby('rule_id').apply(lambda g: ','.join(g['Item Number'].unique()))

    def rule_numbers(self):
        merged = pd.merge(self.grouped.to_frame('rule').reset_index(), 
                          self.rule_counts(), 
                          left_on='rule',
                          right_index=True).sort_values('count', ascending=False)
        return merged.groupby('rule')['Rule #'].apply(lambda s: ','.join(s.astype(int).astype(str)))

    def squash(self):
        print("[INFO] Squashing rules")
        rule_counts = self.rule_counts()
        rule_parts = self.rule_parts()
        rule_order = self.rule_order()
        rule_numbers = self.rule_numbers()
        rules_output = []

        for rule_string, s in rule_counts.iterrows():
            df = pd.DataFrame(self.hydrate_rule(rule_string)).replace('nan', '')
            df['rule_id'] = rule_order.loc[rule_string].rule_order
            df['first_part'] = rule_order.loc[rule_string].Part
            df['rule_numbers'] = rule_numbers.loc[rule_string]
            df['part_numbers'] = rule_parts.loc[s.rule_id]
            df['number_of_appearances'] = rule_counts.loc[rule_string]['count']
            rules_output.append(df)

        rules_output = pd.concat(rules_output)
        rules_output['SEQ #'] = rules_output['SEQ #'].apply(float).apply(int)
        rules_output = rules_output.sort_values(['rule_id', 'SEQ #']).set_index('rule_id')
        self.output = rules_output
        return self
    
    def save(self):
        print("[INFO] Saving output to {filename}".format(filename=self.output_filename))
        rules_output = self.output
        p_rules = rules_output[rules_output['L T'] == 'P']
        c_rules = rules_output[rules_output['L T'] == 'C']
        writer = pd.ExcelWriter(self.output_filename, engine='xlsxwriter')

        p_rules.to_excel(writer, sheet_name='P Rules')
        c_rules.to_excel(writer, sheet_name='C Rules')

        writer.save()
        print("[INFO] Complete.")

def run_all_matching(matchme, output):
    filenames = glob(matchme)
    already_run = list(map(lambda s: s.split("\\")[-1], glob(output+'*')))
    for filename in filenames:
        if filename.split("\\")[-1] not in already_run:
            RuleSquasher(filename, output).load_data().squash().save()
            print()
        else:
            print("[WARNING] Already found target in destination path. Skipping {}".format(filename))
            print()

if __name__ == "__main__":
    if sys.argv[1] == "help":
        print("""
        Welcome to the AIR rule squasher.

        Run this tool with the following arguments

        > python lib/squash_air.py [SOURCE] [TARGET]

        [SOURCE] should have a wildcard matcher (*) at the end.
            Example:  C:/Users/Sargent/Downloads/MyFiles/*.xlsx
            
        [TARGET] should be a folder where the finished files will go.
            Example: C:/Users/Sargent/Downloads/MyFiles/Finished/

        Note file paths are denoted with forward slash ('/') not a
        backward slash ('\\').

        Happy squashing!
        """)
    else:
        print("Squashing all files in", sys.argv[1])
        print("Moving finished files to", sys.argv[2])
        print("--------------")
        run_all_matching(sys.argv[1], sys.argv[2])
