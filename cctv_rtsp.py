#!/opt/local/bin/python3

import os
import datetime
import threading
import pandas as pd
import json
from shutil import copytree
from shutil import copy2
import subprocess


def cctv_json(sites_csv):
    
    """
    Converting sites csv into json for html5 article build. 
    """
    sites = []
    unique_sites = sites_csv['folder'].drop_duplicates().values

    for x in unique_sites:
        devices = sites_csv[sites_csv['folder'] == x]
        devices_names = devices['device_name'].drop_duplicates().tolist()
        sites.append({'site_name': x, 'devices': devices_names})
        
    return sites


def download_file(site_name, device_name, protocol, url, dev_ref):
    
    """
    Collecting snapshots from sites
    """
    timeout = 50
    tname= threading.currentThread().getName()
    directory = 'snaps/' + site_name
    filename = device_name + ".jpg"
    ip = protocol + url + dev_ref
    retryes = 1
    filesize = 0



    # Check if folder exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    directory = directory + "/"

    while retryes<3 and filesize<10000: # number of attempts = 2
        call = "/opt/local/bin/ffmpeg -y -loglevel quiet -rtsp_flags prefer_tcp -i " + ip + " -vframes 1 -r 1 " + directory + filename

        try:
            if os.path.exists(directory + filename):
                print('Removing old file {}{}'.format(directory,filename))
                os.remove(directory + filename)
 
            if retryes == 1: 
                print('Downloading new file {}{}'.format(directory,filename))
            else:
                print('Failed downloading {}{} - retryes {}'.format(directory,filename,retryes))

            subprocess.call(call, shell=True, timeout=timeout)


            print('saving  : {}{}'.format(directory, filename))
            size = os.stat(directory + filename)
            filesize = size.st_size

        except subprocess.TimeoutExpired:
            print('Thread {} Processing {}:{} took too long'.format(tname, site_name,device_name))
        except Exception as e: print(e)
        retryes += 1


# ---------------------------------------------- Main --------------------------------------------

def main():
    
    def_folder = ''
    cctv_dir = '' # to be removed once on Ubuntu.
    backup_dir = def_folder + cctv_dir + "backup"
    snaps_dir = def_folder + cctv_dir + "snaps"
    time_now = datetime.datetime.now().strftime("%Y%m%d_T%H%M%S")
    
    
    # backup directory 
    copytree(snaps_dir, backup_dir + "/snaps" + time_now, symlinks=False, ignore=None, copy_function=copy2) 
    
    
    # Converting csv to Json for html5 article builder. 
    devices = pd.read_csv(def_folder + 'device_list.csv')


    sites = cctv_json(devices)
    with open(def_folder + 'sites.json', 'w+') as yy:
        json.dump(sites, yy)




    # Reding main CSV with all connection detais
    # devices = pd.read_csv(def_folder + 'device_list.csv').T.to_dict()


    sites = devices.folder.drop_duplicates().values
    for s in sites :
        site = devices[devices['folder'] == s]

        for d in site.itertuples():
            site_name = d.folder
            device_name = d.device_name
            protocol = d.protocol
            url = d.url
            dev_ref = d.dev_ref


            t1 = threading.Thread(target=download_file, args=(site_name,device_name,protocol,url,dev_ref))

            t1.start()


if __name__ == '__main__':
    main()
