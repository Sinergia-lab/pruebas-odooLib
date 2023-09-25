import pandas as pd
import numpy as np

def entregable_declaracion_eye(cliente):
    if cliente not in ['JUMBO','SISA','MDH','TXD','CORONA','TOTTUS']:
        raise Exception('Cliente debe ser JUMBO,SISA,MDH, TXD, CORONA o TOTTUS')
    
    if cliente in ['JUMBO','SISA','MDH','TXD']:
        file_input = 'Declaracion_eye_SMK.xlsx'
        file_output = f'Entregable declaracion eye SMK_{cliente}.xlsx'
        index = ['Categoría material','caracterítica del material (solo para plásticos, cartón y vidrio)','Material']
        col_categoria = 'Categoría'

        
    if cliente == 'CORONA':
        file_input = 'Declaracion_eye_corona.xlsx'
        file_output = 'Entregable declaracion eye corona.xlsx'
        index = ['Sub-categoría material','Característica del material','Material']
        col_categoria = 'Categoría'

    
    if cliente=='TOTTUS':
        file_input = 'Declaracion_eye_tottus.xlsx'
        file_output = 'Entregable declaracion eye tottus.xlsx'
        index = ['Sub-categoría material','Característica del material','Material']
        col_categoria = 'Categoría elemento'

    # =============
    # READ FILE
    # =============
    eye_partes = pd.read_excel(file_input)
    eye_partes_nodoc = eye_partes[eye_partes[col_categoria]=='EYE No domiciliario']
    eye_partes_sidoc = eye_partes[eye_partes[col_categoria]=='EYE Domiciliario']

    with pd.ExcelWriter(file_output) as writer:

    # =============
    # DOMICILIARIO
    # =============

        pvt_sidoc = pd.pivot_table(eye_partes_sidoc,'Peso total (ton)',
                            index=index,
                            columns=['Peligrosidad'],aggfunc='sum',fill_value=0)

        pvt_sidoc = np.round(pvt_sidoc,4)
        pvt_sidoc.to_excel(writer,sheet_name='Declaracion lb',startrow=0,startcol=0)

    # =============
    # NO DOMICILIARIO
    # =============

        pvt_nodoc = pd.pivot_table(eye_partes_nodoc,'Peso total (ton)',
                            index=index,
                            columns=['Peligrosidad'],aggfunc='sum',fill_value=0)
        pvt_nodoc = np.round(pvt_sidoc,4)
        pvt_nodoc.to_excel(writer,sheet_name='Declaracion lb',startrow=0,startcol=7)


