import lasio as ls
import os
import logging
import pandas as pd
import json

class logBatch:
    def __init__(self, path):
        with open('mnemonic_dict.json', 'r') as f:
            log_mnemonic_dict = json.load(f)
        self.df = pd.DataFrame(columns=['file']+list(log_mnemonic_dict.keys()))

        df_row = 0
        for f in self.getFiles(path):
            self.df.loc[df_row, 'file'] = f
            if os.path.splitext(f)[1].lower() == '.las':
                try:
                    las = ls.read(f)
                    ls_logs = []
                    for c in las.sections['Curves']:
                        ls_logs.append(c.mnemonic)
                    for m in log_mnemonic_dict.keys():
                        _logs = []
                        for l in ls_logs:
                            if l in log_mnemonic_dict[m]:
                                _logs.append(l)
                            if _logs != []:
                                self.df.loc[df_row, m] = _logs
                    df_row += 1
                except Exception as e:
                    logging.warning(f'Could not read the file: {f}');
                    logging.warning(e)
                    
    def getFiles(self, path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            if filenames:
                for filename in filenames:
                    yield os.path.join(dirpath, filename)    
    
    def filter(self, list_of_logs, inplace=False):
        if inplace:
            self.df = self.df[['file']+list_of_logs].dropna()
            return self.df
        else: 
            return self.df[['file']+list_of_logs].dropna()