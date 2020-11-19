import os

path = r'C:\HazusData\Regions\Joachim_9ft_10ft_2'

for root, dirs, files in os.walk(path, topdown=False):
    for name in files:
        full_path = os.path.join(root, name)
        if full_path.endswith('w001001.adf') and ('rpd' in full_path or 'mix0' in full_path ):
            print(full_path)