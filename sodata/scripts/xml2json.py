from tqdm import tqdm
import xml.etree.ElementTree as ET
import json
import os

int_lim = 1<<63
def try_int(dic):
	for k in dic:
		try:
			dic[k] = int(dic[k]) if -int_lim < int(dic[k]) < int_lim else dic[k]
		except Exception:
			continue
	return dic

def xml2json(data_dir, verbose=True):
    for fname in os.listdir(data_dir):
        if fname[-4:] == '.xml':
            fin_name = os.path.join(data_dir, fname)
            fout_name = fin_name[:-4] + '.json'
            with open(fin_name, 'r') as fin, \
                open(fout_name, 'w') as fout:
                if verbose:
                    fin = tqdm(fin, desc=fin_name)
                for line in fin:
                    try:
                        fout.write(json.dumps(try_int(ET.fromstring(line).attrib)))
                        fout.write('\n')
                    except ET.ParseError:
                        pass
