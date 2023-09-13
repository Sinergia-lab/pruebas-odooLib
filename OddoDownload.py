import odoolib
import pandas as pd
import numpy as np
import os
from tqdm import tqdm


class OdooDownloadBase:
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
        
    def gestionarListas(self,registro,campos_fk):
        """
        Al descargar registros del modelo, cuando un campo es una llave foranea de descarga como lista [llave, valor_referencia]. Se realiza un join implicito.
        limpiarLlaves permite eliminar la llave y quedarse solo con el valor legible

        Parametros:
        - registro: dict{} que representa un registro (fila) de la tabla

        Returns: 
        - dict{} qye representa un registro (fila) de la tabla pero sin llave en el campo especificado
        """
        campos = list(registro.keys())

        for nombre_campo in campos:
            if (nombre_campo in campos_fk) and type(registro[nombre_campo]) == list:
                registro[nombre_campo] = registro[nombre_campo][1]
            elif type(registro[nombre_campo]) == list:
                registro[nombre_campo] = str(registro[nombre_campo])
        return registro
    
    def getDataChunk(self,model_conn,lista_filtros,lista_campos,offset,campos_fk):
        """
        Descarga un chunk(lote) de datos. Las tablas muy grandes se dividen en lotes.

        Parametros:
        - model_conn: Conexion con el modelo. Se obtiene con get_model
        - lista_filtros: list[] con los filtros a aplicar en la busqueda. Cada filtro representa una sentencia where de sql
        - lista_campos: list[] con los campos a traer desde el modelo
        - offset: offset. Se descargaran x cantidad de dato desde 0 + offset

        """
        res = model_conn.search_read(lista_filtros,lista_campos,limit=self.chunk_size,offset=offset)         # get data
        res = list( map(lambda x:self.gestionarListas(x,campos_fk),res) )
        res = list( map(lambda x:list(x.values()),res) )                                                      # pasar valores de dict a list
        return np.array(res)

    def getDataFromModel(self,modelo,lista_filtros,lista_campos,header=None,ret_=False,campos_fk=[],drop_id=True):
        """
        Descarga los registros desde un modelo almacenado en odoo

        Parametros:
        - modelo: str con el nombre del modelo
        - lista_filtros: list[] con los filtros a aplicar en la busqueda. Cada filtro representa una sentencia where de sql
        - lsita_campos: list[] con los campos a traer desde el modelo
        - header: list[] con los nombres de las columnas a insertar en el archivo final. Si es False, entonces se utiliza el nombre de los campos
        - ret_: bool, si es True entonces en vez de guardarse en como atributo en la clase, la tabla es retornada

        Returns:
        - Ninguno: El documento generado se guarda como variable dentro del objeto odooDownload
        - Si res_ es True entonces se returno un dataframe
        """

        # DESCARGAR DATOS
        self.resultadoBusqueda = None
        model_conn = self.conexion.get_model(modelo)
        
        res = None
        offset = 0
        print('Descargando data desde',modelo)
        while True:
            print('.')
            res_ = self.getDataChunk( model_conn,lista_filtros,lista_campos,offset,campos_fk )
            
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
        if not len(res):        res = pd.DataFrame(columns=header)
        else:                   res = pd.DataFrame(data=res,columns=header)

        if drop_id:
            res = res.drop(['id'],axis=1)

        if ret_:        return res
        else:           self.resultadoBusqueda = res

    
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
            
    def quitarTrueFalse(self,df,campos,to_replace=''):
        for campo in campos:
            df[campo] = df[campo].replace('True',to_replace)
            df[campo] = df[campo].replace('False',to_replace)
        return df
    # ============================
    # PLANTILLAS PREDEFINIDAS
    # ============================

class OdooDownloadCenco(OdooDownloadBase):
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
        header = ['SKU unidad negocio','SKU','Etapa','EVA','Analisis fisico']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['SKU unidad negocio','SKU']
        
        self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)
        self.downloadExcel('Maestra','csv')
        print('Se ha generado el archivo Maestra.csv')
    
    def comunicacion_masiva(self,anho,unidad_negocio):
        """
        Descarga la tabla de comunicacion masiva para smk

        Parametros:
        - anho: Periodo que se quiere descargar

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """

        # PARAMETROS
        filtro1 = ["&","&",["x_studio_periodos.x_name","=",anho],["x_studio_unidades_de_negocio","=",unidad_negocio],"|",["x_studio_stage_id","=",2],["x_studio_stage_id","=",5]]
        filtro2 = ["&","&","&",["x_studio_periodos.x_name","=",anho],["x_studio_unidades_de_negocio","=",unidad_negocio],["x_studio_stage_id","=",3],["x_studio_variable_de_marcado","=",1]]
        filtro3 = ["&","&",["x_studio_periodos","=",False],["x_studio_unidades_de_negocio","=",unidad_negocio],"|",["x_studio_stage_id","=",2],["x_studio_stage_id","=",5]]
        campos = ['x_studio_sku_unidad_de_negocio','x_studio_cdigo_regional','x_studio_descripcin','x_studio_ean','x_studio_proveedor','x_studio_equipo',
          'x_studio_pm_asociado','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        modelo = 'x_productos'
        header = ['SKU unidad negocio','Codigo regional','Descripcion','EAN','Proveedor','Equipo','PM asociado','Trazabilidad levantamiento','Etapa']
        campos_fk = ['x_studio_proveedor','x_studio_equipo','x_studio_pm_asociado','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        borrar_truefalse = ['EAN','Trazabilidad levantamiento','Codigo regional']

        # DESCARGA DE TABLAS
        self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp1',formato='csv')
        self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp2',formato='csv')
        self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp3',formato='csv')

        # JUNTAR TABLAS
        f1 = pd.read_csv('temp1.csv')
        f2 = pd.read_csv('temp2.csv')
        f3 = pd.read_csv('temp3.csv')
        f_final = pd.concat([f1,f2,f3],axis=0,ignore_index=1)
        # ADJUNTA CORREO
        actores = self.getDataFromModel('x_actores_relevantes',[],['x_name','x_studio_partner_email'],ret_=True)
        lista_correos = []                                                              
        for pm in f_final['PM asociado']:
            try:
                email = actores[actores['x_name']==pm]['x_studio_partner_email'].values[0]
                lista_correos.append(email)
            except:
                lista_correos.append(False)
        lista_correos = pd.Series(lista_correos)
        f_final['PM asociado/Correo electrónico'] = lista_correos
        f_final = f_final[['SKU unidad negocio','Codigo regional','Descripcion','EAN','Proveedor','Equipo','PM asociado','PM asociado/Correo electrónico','Trazabilidad levantamiento','Etapa']]
        
        # LIMPIA TRUE-FALSE Y GUARDA
        f_final = self.quitarTrueFalse(f_final,borrar_truefalse)
        f_final.to_csv('Comunicacion masiva.csv',index=False)

        # BORRAR ARCHIVOS TEMPORALES
        os.remove('temp1.csv')
        os.remove('temp2.csv')
        os.remove('temp3.csv')

        print('Se ha generado el archivo Comunicacion masiva.csv')

    def declaracion_eye(self,unidad_negocio,periodo):
        if unidad_negocio not in ['JUMBO','SISA']:
            raise Exception('La unidad de negocio debe ser "JUMBO" o "SISA"')
        
        if unidad_negocio=='JUMBO':
            campos_ventas_totales = ['x_studio_total_conveniencia','x_studio_total_jumbo']
            header_ventas_totales = ['TOTAL CONVENIENCIA','TOTAL JUMBO']
        elif unidad_negocio=='SISA':
            campos_ventas_totales = ['x_studio_total_sisa']
            header_ventas_totales = ['TOTAL SISA']
        
        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================
        modelo = 'x_ventas'
        filtros = ['&',('x_studio_unidades_de_negocio','=','SMK'),('x_studio_periodo.x_name','=',periodo)]
        campos = ['x_studio_sku_unidad_de_negocio','x_studio_descripcin_producto','x_studio_elementos_del_producto'] + campos_ventas_totales
        header = ['Producto','Producto/Descripción','lista_elementos'] + header_ventas_totales 
        campos_fk = ['x_studio_producto']

        ventas = self.getDataFromModel(modelo,filtros,campos,ret_=True,campos_fk=campos_fk, header=header)
        
        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # ============================
        total_elementos = []
        for i in range(len(ventas)):
            elementos = eval(ventas['lista_elementos'].iloc[i])
            total_elementos += elementos
        len(set(total_elementos))

        # ===========================
        # DESCARGAR DETALLE DE PARTES
        # ===========================
        modelo = 'x_materialidad'
        filtros = [("id","in",total_elementos)]
        campos = ['x_name','x_studio_productos_por_envase','x_studio_peso','x_studio_peso_informado','x_studio_mat',
                'x_studio_caractertica_del_material_solo_para_plsticos','x_studio_definir_otro_material','x_studio_caracterstica_reciclable',
                'x_studio_caracteristica_retornable','x_studio_peligrosidad','x_studio_categora','x_studio_cat_material']

        header = ['Elementos del producto','Productos por envase','Peso','Peso informado','Material','caracterítica del material (solo para plásticos, cartón y vidrio)',
                'Definir otro material','Característica reciclable','Característica retornable','Peligrosidad','Categoría','Categoría material']
        
        campos_fk = ['x_studio_mat']

        materialidad = self.getDataFromModel(modelo,filtros,campos,ret_=True,drop_id=False,campos_fk=campos_fk,header=header)
        materialidad['id'] = pd.to_numeric(materialidad['id'], errors='coerce')

        # ===========================
        # CREAR TABLA FINAL
        # ===========================

        header_1 = ['Producto','Producto/Descripción']
        header_2 = ['Elementos del producto','Productos por envase','Peso','Peso informado','Material','caracterítica del material (solo para plásticos, cartón y vidrio)',
                'Definir otro material','Característica reciclable','Característica retornable','Peligrosidad','Categoría','Categoría material']
        header_3 = header_ventas_totales
        final_header = header_1+header_2+header_3

        n_campos = len(final_header)
        declaracion_eye = np.zeros( (len(total_elementos),n_campos),dtype='object' )

        index_declaracion = 0
        for i in tqdm(range(len(ventas))):                                      # POR CADA FILA EN VENTAS (tabla x_ventas)
            lista_elementos = eval(ventas.iloc[i].lista_elementos)              # OBTENGO LOS ELEMENTOS (o partes xd)              
            parte1 = ventas[header_1].iloc[i].to_numpy().reshape(1,-1)  
            parte3 = ventas[header_3].iloc[i].to_numpy().reshape(1,-1)

            for elemento in lista_elementos:                                    # POR CADA PARTE
                detalle_elemento = materialidad[ materialidad['id']==elemento ]
                detalle_elemento = detalle_elemento[header_2].to_numpy().reshape(1,-1)
                row_declaracion = np.concatenate([parte1,detalle_elemento,parte3],axis=1)

                declaracion_eye[index_declaracion] = row_declaracion            # ANADE EL ELEMENTO A LA TABLA FINAL
                index_declaracion += 1

        declaracion_eye = pd.DataFrame(data=declaracion_eye,columns=final_header)
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría']=='EYE Domiciliario') | (declaracion_eye['Categoría']=='EYE No domiciliario')]
        declaracion_eye = declaracion_eye.replace('False','')

        # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================

        if unidad_negocio=='JUMBO':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL JUMBO']=declaracion_eye['TOTAL JUMBO'].astype('float')
            declaracion_eye['TOTAL CONVENIENCIA']=declaracion_eye['TOTAL CONVENIENCIA'].astype('float')

            declaracion_eye['Peso total (gr)'] = (declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*(declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*(declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']
        elif unidad_negocio=='SISA':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL SISA']=declaracion_eye['TOTAL SISA'].astype('float')

            declaracion_eye['Peso total (gr)'] = declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']
            
        # ==================================                        
        # DESCARGAR
        # ==================================
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(f'Declaracion_eye_smk_{unidad_negocio}','xlsx')

class OdooDownloadCorona(OdooDownloadBase):

    def maestra(self):
        modelo = 'x_productos'
        filtros = []
        campos = ['x_name','x_studio_stage_id']
        header = ['Name','Etapa']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['Name']

        self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)

        self.downloadExcel('Maestra','csv')
        print('Se ha generado el archivo Maestra.csv')
    
    def maestra_homologos(self):
        modelo = 'x_productos'
        filtros = []
        campos = ['x_studio_ean','x_studio_estilo_1','x_name','x_studio_url','x_studio_hoja','x_studio_notes']
        header = ['EAN','Estilo','Name','URL','HOJA','Notas']
        campos_fk = []
        borrar_truefalse = ['EAN','Estilo','Name','URL','HOJA','Notas']

        self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)

        self.downloadExcel('Maestra_homologos','csv')
        print('Se ha generado el archivo Maestra_homologos.csv')        

    def comunicacion_masiva(self,periodo):
        no_completado_nuevo = 1
        no_completado_revision = 2
        completado = 3
        proyectado = 4
        completado_parcial = 5

        modelo = 'x_productos'
        campos = ['x_name','x_studio_estilo_1','x_studio_descripcin_larga','x_studio_ean','x_studio_proveedor','x_studio_pm_asociado','x_studio_productos_trazabilidad','x_studio_stage_id']
        header = ['Name','Estilo','Descripción larga','EAN','Proveedor','PM asociado','Trazabilidad levantamiento','Etapa']
        campos_fk = ['x_studio_proveedor','x_studio_pm_asociado','x_studio_productos_trazabilidad','x_studio_stage_id']
        borrar_truefalse = ['EAN','Trazabilidad levantamiento']


        filtro1 = ["&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]
        filtro2 = ["&","&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id","=",completado],["x_studio_aux","=",1]]
        filtro3 = ["&",["x_studio_periodos","=",False],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]

        self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp1',formato='csv')
        self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp2',formato='csv')
        self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk)
        self.downloadExcel('temp3',formato='csv')

        # JUNTAR TABLAS
        f1 = pd.read_csv('temp1.csv')
        f2 = pd.read_csv('temp2.csv')
        f3 = pd.read_csv('temp3.csv')
        f_final = pd.concat([f1,f2,f3],axis=0,ignore_index=1)

        # ADJUNTA CORREO
        actores = self.getDataFromModel('x_actores_relevantes',[],['x_name','x_studio_partner_email'],ret_=True)
        lista_correos = []                                                              
        for pm in f_final['PM asociado']:
            try:
                email = actores[actores['x_name']==pm]['x_studio_partner_email'].values[0]
                lista_correos.append(email)
            except:
                lista_correos.append(False)
        lista_correos = pd.Series(lista_correos)
        f_final['PM asociado/Correo electrónico'] = lista_correos
        f_final = f_final[['Name','Estilo','Descripción larga','EAN','Proveedor','PM asociado','PM asociado/Correo electrónico','Trazabilidad levantamiento','Etapa']]

        # DESCARGAR EXCEL
        f_final = self.quitarTrueFalse(f_final,borrar_truefalse)
        f_final.to_csv('Comunicacion masiva.csv',index=False)

        # BORRAR ARCHIVOS TEMPORALES
        os.remove('temp1.csv')
        os.remove('temp2.csv')
        os.remove('temp3.csv')

        print('Se ha generado el archivo Comunicacion masiva.csv')

    def declaracion_eye(self):
        pass