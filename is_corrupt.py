from rucio.client.client import Client
from pathlib import Path
import pandas as pd
import gfal2
import argparse

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='is_corrupt')
        self.parser.add_argument('filename', help='specify filename as <scope:file>', action='store')

        
def get_replicas(args):
    scope, name = args.filename.split(':')
    did = [{'scope':scope,'name':name}]
    df = pd.DataFrame(client.list_replicas(did, all_states=True))
    checksum = df['adler32'][0]
    filesize = df['bytes'][0]
    rse = []
    state = []
    for i, replica  in enumerate(list(df['pfns'][0].keys())):
        rse.append(df['pfns'][0][replica]['rse'])
        state.append(df['states'][0][rse[i]])
    dit = {'replica':list(df['pfns'][0].keys()),
           'RSE': rse,
           'state': state,
          }
    df = pd.DataFrame(dit)
    end = len(df.columns)
    df.insert(end, 'reason', '')
    df.insert(end, 'diag', '')
    
    return df, filesize, checksum
        
def check(replica, rse, filesize, checksum):
    try:
        ctxt.checksum(replica,'adler32')
        if filesize == ctxt.stat(replica).st_size:
            if checksum == ctxt.checksum(replica,'adler32'):
                diag = 'ok'
                reason = 'file passed all tests'
            else:
                print('Checksum incorrect!')
                print('Expected ' + str(checksum) + ' and got ' + str(ctxt.checksum(replica,'adler32')))
                diag = 'file corrupt'
                reason = 'file checksum mismatch'
        else:
            print('Filesize incorrect!')
            print('Expected ' + str(filesize) + ' and got ' + str(ctxt.stat(replica).st_size))
            diag = 'file corrupt'
            reason = 'filesize mismatch'
    except:
        print('File not found on ' + rse)
        diag = 'file missing'
        reason = 'file not found'
        
    return diag, reason
        
        
def is_corrupt(args):
    df, filesize, checksum = get_replicas(args)
    print('Running remote tests...')
    for i in df.index:
        df['diag'][i], df['reason'][i] = check(df['replica'][i], df['RSE'][i], filesize, checksum)
    
    print('Copying files and running tests locally...')
    p = Path('/tmp/temp.root')
    
    for i in df.loc[df['diag'] == 'ok'].index:
        try:
            t = ctxt.transfer_parameters()
            ctxt.filecopy(t, df['replica'][i], 'file://' + str(p))
            df['diag'][i], df['reason'][i] = check('file://' + str(p), df['RSE'][i], filesize, checksum)
            p.unlink()
        except: 
            print('Replica from ' + df['RSE'][i] + ' failed to copy!')
            df['diag'][i] = 'unknown'
            df['reason'][i] = 'disk copy unavailable for copy but replica passed remote checksum and filesize checks'

    print('Tests complete!')
    print('Results:')
    print(df)
    
    for i in df.loc[(df['diag'] != 'ok') & (df['diag'] != 'unknown')].index:
        print('The replica at ' + df['RSE'][i] + 
              'is likely corrupt. This script returned "' + df['reason'][i] + '" as the problem.')
        print('Would you like to delete the file?')
        print('Type "yes" if you would like to delete.')
        delete = input('')
        if delete == 'yes':
            client.delete_replicas(df['RSE'][i], [args.filename])
            print(args.filename + ' deleted at ' + df['RSE'][i])

if __name__ == '__main__':
    print('Be assured that the code is running')
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    ctxt = gfal2.creat_context()
    client = Client()
    is_corrupt(args)