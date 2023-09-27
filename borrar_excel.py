import os

files = os.listdir('archivos_generados/')
for file in files:
    extencion = file.split('.')[-1]
    if extencion=='csv' or extencion=='xlsx':
        os.remove('archivos_generados/'+file)