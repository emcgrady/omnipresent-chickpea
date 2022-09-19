from __future__ import division, print_function
from rucio.client.client import Client
from rucio.common.exception import RuleNotFound
import pandas as pd
import argparse
import datetime
import time

"""
rule states:
    REPLICATING = 'R'
    OK = 'O'
    STUCK = 'S'
    SUSPENDED = 'U'
    WAITING_APPROVAL = 'W'
    INJECT = 'I'
    ALL = 'A'
"""

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='save_staging_rules')
        self.parser.add_argument('--states', help='specify rule state', default='A', type=list, dest='states')
        self.parser.add_argument('--start', help='First date to look for staging rules (YYYY-MM-DD)', required=True, dest='start')
        self.parser.add_argument('--end', help='Last date to look for staging rules (YYYY-MM-DD)', default=datetime.datetime.now(), dest='end')
        self.parser.add_argument('--file_name', help='location to save the parquet file', default='staging_rules.parquet', dest='file_name')
        self.parser.add_argument('--rse', help='Filter by RSE expression', default=None, dest='rse')
        self.parser.add_argument('--account', help='Filter by account', default=None, dest='account')
        
        
def save_stuck_rules(args):
    start = datetime.datetime.strptime(args.start, '%Y-%m-%d')
    rse = args.rse
    account = args.account
    if type(args.end) == str:
        end = datetime.datetime.strptime(args.end, '%Y-%m-%d')
    else:
        end = args.end
    df = pd.DataFrame()
    
    if args.states == ['A']:
        states = ['R','S','U', 'I', 'W']
        
    else:
        states = args.states
        
    for state in states:
        rules = client.list_replication_rules(filters={'state': state})
        temp = pd.DataFrame(rules)
        temp = temp.loc[(temp['created_at'] < end) & (temp['created_at'] > start)].reset_index(drop=True)
        if rse != None:
            temp = temp.loc[temp['rse_expression'] == rse]
        if account != None:
            temp = temp.loc[temp['account'] == account]
        temp = temp[['state', 'id', 'account', 'name', 'rse_expression', 'updated_at']]
        df = pd.concat([df, temp], ignore_index=True)
        
    df.to_parquet(args.file_name)
    
if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    save_stuck_rules(args)
        