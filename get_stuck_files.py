from rucio.client.client import Client
import pandas as pd
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='get_stuck_files')
        self.parser.add_argument('--id', help='specify rule id', required=True, dest='rule_id')
        self.parser.add_argument('--file_name', help='location to save the txt file', default='stuck_files.txt', dest='file_name')
        
def get_stuck_files(args):
    rule_id = args.rule_id
    temp = client.get_replication_rule(rule_id)
    df = pd.DataFrame(client.list_content(temp['scope'], temp['name']))
    datasets = pd.DataFrame()
    final = pd.DataFrame()
    for i in range(len(df)):
        if df['name'].str.endswith('.file0001')[i]:
            datasets = pd.concat([datasets, df['name']], ignore_index=True)
        else:
            temp = pd.DataFrame(client.list_dataset_replicas(df['scope'][i], df['name'][i]))
            temp = temp.loc[temp['state'] != 'AVAILABLE']
            if len(temp != 0):
                datasets = datasets.append(temp)
    datasets = datasets.reset_index()
    for i in range(len(datasets)):
        temp = pd.DataFrame(client.list_content(datasets['scope'][i], datasets['name'][i]))
        final = final.append(temp)
    final = final.reset_index()
    final = final['name']
    final.to_csv(args.file_name, header=None, index=None, sep=' ')
    
if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_stuck_files(args)