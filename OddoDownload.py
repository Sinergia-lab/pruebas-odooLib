import odoolib
import pandas as pd
import numpy as np

class OddoDownload:
    def __init__(self,conn_params):
        self.conn_params = conn_params
        self.connect()
        self.resultadoBusqueda = None
        self.chunk_size = 50000
    
    def connect(self):
        """
        Establece la comunicacion con la base de datos
        """
        self.conexion = odoolib.get_connection(
                hostname=self.conn_params['ODOO_HOSTNAME'],
                database=self.conn_params['ODOO_DATABASE'],
                login=self.conn_params['ODOO_USERNAME'],
                password=self.conn_params['ODOO_PASSWORD'],
                port=443,
                protocol='jsonrpcs')
        
    def limpiarLlaves(self,registro):
        """
        Al descargar registros del modelo, cuando un campo es una llave foranea de descarga como lista [llave, valor_referencia]. Se realiza un join implicito.
        limpiarLlaves permite eliminar la llave y quedarse solo con el valor legible

        Parametros:
        - registro: dict{} que representa un registro (fila) de la tabla

        Returns: 
        - dict{} qye representa un registro (fila) de la tabla pero sin llave en el campo especificado
        """
        campos_fk = self.getLlavesForaneas(registro)
        for nombre_campo in campos_fk:
            if len(registro[nombre_campo]) == 2:
                registro[nombre_campo] = registro[nombre_campo][1]
            else:
                registro[nombre_campo] = str(registro[nombre_campo])
        return registro
           
    
    def getLlavesForaneas(self,registro):
        """
        Encuentra cuales campos son listas con llaves foraneas [llave, valor_referencia]

        Parametros:
        - registro: dict{} con un registro.

        Returns:
        - list[] de strings con los nombres de los campos que son listas con llaves foraneas
        """
        fks = []
        for campo in registro.keys():
            if type(registro[campo]) == list:
                fks.append(campo)
        return fks
    
    def getDataChunk(self,model_conn,lista_filtros,lista_campos,offset):
        """
        """
        res = model_conn.search_read(lista_filtros,lista_campos,limit=self.chunk_size,offset=offset)         # get data
        res = list( map(self.limpiarLlaves,res) )                                                             # limpiar llaves foraneas
        res = list( map(lambda x:list(x.values()),res) )                                                      # pasar valores de dict a list
        return np.array(res)

    def getDataFromModel(self,modelo,lista_filtros,lista_campos,header=None):
        """
        Descarga los registros desde un modelo almacenado en odoo

        Parametros:
        - modelo: str con el nombre del modelo
        - lista_filtros: list[] con los filtros a aplicar en la busqueda. Cada filtro representa una sentencia where de sql
        - lsita_campos: list[] con los campos a traer desde el modelo
        - header: list[] con los nombres de las columnas a insertar en el archivo final. Si es False, entonces se utiliza el nombre de los campos

        Returns:
        - Ninguno: El documento generado se guarda como variable dentro del objeto odooDownload
        """

        # DESCARGAR DATOS
        self.resultadoBusqueda = None
        model_conn = self.conexion.get_model(modelo)
        
        res = None
        offset = 0
        print('Descargando data')
        while True:
            print('.')
            res_ = self.getDataChunk( model_conn,lista_filtros,lista_campos,offset )
            
            if type(res) != np.ndarray:  res = res_
            else:                        res = np.concatenate( [res,res_],axis=0 )

            offset += self.chunk_size
            if len(res_) < self.chunk_size:
                break

        print('done')

        # SET HEADER
        if not header and len(lista_campos):    header = ['id'] + lista_campos      # NO HAY HEADER PERO HAY LISTA DE CAMPOS
        elif not header:                        header = res[0].keys()              # NO HAY NI HEADER NI LISTA DE CAMPOS
        else:                                   header = ['id'] + header            # HAY HEADER

        # SAVE DATAFRAME
        res = pd.DataFrame(data=res,columns=header)
        # res = res[ header[1:] ]     # QUITAR EL ID

        self.resultadoBusqueda = res

    
    def downloadExcel(self,ruta,formato='xlsx'):
        """
        Genera el archivo final en el disco duro

        Parametros:
        - ruta: str con la ruta final del archivo. Si la ruta es solo un nombre, el archivo se genera en la misma carpeta
        - formato: str con el formato del archivo. Los valores validos son 'xlsx' o 'csv'

        Returns:
        - Ninguno: Se genera un archivo en el disco duro
        """
        if type(self.resultadoBusqueda) != pd.core.frame.DataFrame:
            print('No se ha descargado ningun modelo o no se encontraron registros')
            return
        
        print('Generando archivo')
        if formato=='xlsx':     self.resultadoBusqueda.to_excel(f'{ruta}.xlsx',index=False)
        elif formato=='csv':    self.resultadoBusqueda.to_csv(f'{ruta}.csv',index=False)
        else:                   raise Exception('Los formatos de archivo validos son "xlsx" y "csv"')
            

    def maestra(self,unidad_negocio):
        """
        Shortcut con parametros para descargar la tabla Maestra

        Parametros: 
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        
        Returns:
        - Ninguno: El documento generado se guarda como variable dentro del objeto odooDownload
        """
        modelo = 'x_productos'
        filtros = [('x_studio_unidades_de_negocio','=',unidad_negocio)]
        campos = ['x_studio_sku_unidad_de_negocio','x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_candidato_a_analisis_fisico']
        header = ['SKU unidad negocio','SKU','Etapa','EVA','Analisis fisivo']
        self.getDataFromModel(modelo,filtros,campos,header)

