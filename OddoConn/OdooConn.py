import odoolib
import pandas as pd
import numpy as np

class OddoConn:
    def __init__(self,conn_params):
        self.listas_recurrentes = {
            'MAESTRA':['x_studio_sku_unidad_de_negocio','x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_candidato_a_analisis_fisico']
        }
        self.conn_params = conn_params
        self.connect()
        self.resultadoBusqueda = None
    
    def connect(self):
        self.conexion = odoolib.get_connection(
                hostname=self.conn_params['ODOO_HOSTNAME'],
                database=self.conn_params['ODOO_DATABASE'],
                login=self.conn_params['ODOO_USERNAME'],
                password=self.conn_params['ODOO_PASSWORD'],
                port=443,
                protocol='jsonrpcs')
        
    def limpiar_resultados(self,x):
        x['x_studio_stage_id'] = x['x_studio_stage_id'][1]
        return x
    
    def getDataFromModel(self,modelo,lista_filtros,lista_campos,header=None):

        # SET HEADER
        if not header:  header = ['id'] + lista_campos
        else:           header = ['id'] + header

        # OBTENER DATOS
        res = self.conexion.get_model(modelo)
        res = res.search_read(lista_filtros,lista_campos,limit=5)
        res = list( map(self.limpiar_resultados,res) )

        # CREAR DATAFRANE
        res = list( map(lambda x:list(x.values()),res) )
        res = np.array(res)
        res = pd.DataFrame(data=res,columns=header)

        return res
    
    def downloadExcel(self):
        pass

    def maestra(self,empresa):
        modelo = 'x_productos'
        filtros = [('x_studio_unidades_de_negocio','=',empresa)]
        campos = ['x_studio_sku_unidad_de_negocio','x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_candidato_a_analisis_fisico']
        header = ['SKU unidad negocio','SKU','Etapa','EVA','Analisis fisivo']
        return self.getDataFromModel(modelo,filtros,campos,header)

