import os

files = os.listdir()
for file in files:
    extencion = file.split('.')[-1]
    if extencion=='csv' or extencion=='xlsx':
        os.remove(file)