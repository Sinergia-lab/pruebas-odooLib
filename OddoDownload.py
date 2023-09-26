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
        # combinacion = (header is not None, len(lista_campos)!=0, 'id' in lista_campos)  # Hay header, hay campos, se solicita el id
        # if combinacion == (0,1,0):  header=['id'] + lista_campos
        # elif combinacion==(0,1,1):  header=lista_campos
        # elif combinacion==(1,1,0):  header=['id']+header
        # elif combinacion==(1,1,1):  header=header
        # else:
        #     raise Exception('La combinancion de header, campos y id no permite calcular el tamano correcto del header')

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

    def downloadExcel(self,filename,defaultname,formato='xlsx'):
        """
        Genera el archivo final en el disco duro

        Parametros:
        - ruta: str con la ruta final del archivo. Si la ruta es solo un nombre, el archivo se genera en la misma carpeta
        - formato: str con el formato del archivo. Los valores validos son 'xlsx' o 'csv'

        Returns:
        - Ninguno: Se genera un archivo en el disco duro
        """
        if type(self.resultadoBusqueda) != pd.core.frame.DataFrame:
            raise Exception('No se ha descargado ningun modelo o no se encontraron registros')
        
        filename = filename if filename else defaultname
        
        print('Generando archivo')
        if formato=='xlsx':     self.resultadoBusqueda.to_excel(f'{filename}.xlsx',index=False)
        elif formato=='csv':    self.resultadoBusqueda.to_csv(f'{filename}.csv',index=False)
        else:                   raise Exception('Los formatos de archivo validos son "xlsx" y "csv"')

        print(f'Se ha generado el archivo {filename}.{formato}')
            
    def quitarTrueFalse(self,df,campos,to_replace=''):
        for campo in campos:
            df[campo] = df[campo].replace(False,to_replace)
            df[campo] = df[campo].replace('False',to_replace)
        return df
# ============================
# PLANTILLAS PREDEFINIDAS
# ============================

class OdooDownloadCenco(OdooDownloadBase):
    def maestra(self,unidad_negocio,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """

        if unidad_negocio not in ['SMK','MDH','TXD']:
            raise Exception('La unidad de negocio debe ser SMK, MDH o TXD')

        modelo = 'x_productos'
        filtros = [('x_studio_unidades_de_negocio','=',unidad_negocio)]
        campos = ['x_studio_sku_unidad_de_negocio','x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_candidato_a_analisis_fisico']
        header = ['SKU unidad negocio','SKU','Etapa','EVA','Analisis fisico']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['SKU unidad negocio','SKU']
        
        self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)
        self.downloadExcel(filename,f'Maestra Cenco-{unidad_negocio}','csv')
    
    def comunicacion_masiva(self,anho,unidad_negocio,filename=None):
        """
        Descarga la tabla de comunicacion masiva.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """

        if unidad_negocio not in ['SMK','MDH','TXD']:
            raise Exception('La unidad de negocio debe ser "SMK", MDH o TXD')

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

        f1 = self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk,ret_=True)
        f2 = self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk,ret_=True)
        f3 = self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk,ret_=True)
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
        self.resultadoBusqueda = self.quitarTrueFalse(f_final,borrar_truefalse)
        self.downloadExcel(filename,f'Comunicacion masiva Cenco-{unidad_negocio}','csv')

    def declaracion_eye(self,unidad_negocio,periodo,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """

        if unidad_negocio not in ['JUMBO','SISA','MDH','TXD']:
            raise Exception('La unidad de negocio debe ser "JUMBO", "SISA", MDH o TXD')

        # ===========================
        # PREPARAR HEADERS ADECUADOS
        # ===========================        
        unidad_negocio_original = None

        if unidad_negocio=='JUMBO':
            campos_ventas_totales = ['x_studio_total_conveniencia','x_studio_total_jumbo']
            header_ventas_totales = ['TOTAL CONVENIENCIA','TOTAL JUMBO']
            unidad_negocio = 'SMK'
            unidad_negocio_original = 'JUMBO'

        elif unidad_negocio=='SISA':
            campos_ventas_totales = ['x_studio_total_sisa']
            header_ventas_totales = ['TOTAL SISA']
            unidad_negocio = 'SMK'
            unidad_negocio_original = 'SISA'


        elif unidad_negocio=='MDH':
            campos_ventas_totales=['x_studio_total_easy']
            header_ventas_totales=['TOTAL EASY']
        
        elif unidad_negocio=='TXD':
            campos_ventas_totales=['x_studio_total_paris']
            header_ventas_totales=['TOTAL PARIS']
        
        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================
        modelo = 'x_ventas'
        filtros = ['&',('x_studio_unidades_de_negocio','=',unidad_negocio),('x_studio_periodo.x_name','=',periodo)]
        campos = ['x_studio_producto','x_studio_descripcin_producto','x_studio_elementos_del_producto'] + campos_ventas_totales
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

        if unidad_negocio=='SMK' and unidad_negocio_original=='JUMBO':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL JUMBO']=declaracion_eye['TOTAL JUMBO'].astype('float')
            declaracion_eye['TOTAL CONVENIENCIA']=declaracion_eye['TOTAL CONVENIENCIA'].astype('float')
            declaracion_eye['Peso total (gr)'] = (declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*(declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*(declaracion_eye['TOTAL JUMBO']+declaracion_eye['TOTAL CONVENIENCIA'])*declaracion_eye['Peso']

        elif unidad_negocio=='SMK' and unidad_negocio_original=='SISA':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL SISA']=declaracion_eye['TOTAL SISA'].astype('float')
            declaracion_eye['Peso total (gr)'] = declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['TOTAL SISA']*declaracion_eye['Peso']

        elif unidad_negocio=='MDH':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL EASY']=declaracion_eye['TOTAL EASY'].astype('float')
            declaracion_eye['Peso total (gr)'] = declaracion_eye['TOTAL EASY']*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['TOTAL EASY']*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['TOTAL EASY']*declaracion_eye['Peso']

        elif unidad_negocio=='TXD':
            declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
            declaracion_eye['TOTAL PARIS']=declaracion_eye['TOTAL PARIS'].astype('float')
            declaracion_eye['Peso total (gr)'] = declaracion_eye['TOTAL PARIS']*declaracion_eye['Peso']
            declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['TOTAL PARIS']*declaracion_eye['Peso']
            declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['TOTAL PARIS']*declaracion_eye['Peso']
            
        # ==================================                        
        # DESCARGAR
        # ==================================
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,f'Declaracion_eye Cenco-{unidad_negocio}','xlsx')

class OdooDownloadCorona(OdooDownloadBase):

    def maestra(self,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """
        modelo = 'x_productos'
        filtros = []
        campos = ['x_name','x_studio_stage_id']
        header = ['Name','Etapa']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['Name']

        self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)

        self.downloadExcel(filename,f'Maestra Corona','csv')

    def comunicacion_masiva(self,periodo,filename=None):
        """
        Descarga la tabla de comunicacion masiva

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """
        # CODIGOS DE ETAPA DE ESTUDIOS
        no_completado_nuevo = 1
        no_completado_revision = 2
        completado = 3
        proyectado = 4
        completado_parcial = 5

        # parametros de descarga
        modelo = 'x_productos'
        campos = ['x_name','x_studio_estilo_1','x_studio_descripcin_larga','x_studio_ean','x_studio_proveedor','x_studio_pm_asociado','x_studio_productos_trazabilidad','x_studio_stage_id']
        header = ['Name','Estilo','Descripción larga','EAN','Proveedor','PM asociado','Trazabilidad levantamiento','Etapa']
        campos_fk = ['x_studio_proveedor','x_studio_pm_asociado','x_studio_productos_trazabilidad','x_studio_stage_id']
        borrar_truefalse = ['EAN','Trazabilidad levantamiento']


        filtro1 = ["&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]
        filtro2 = ["&","&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id","=",completado],["x_studio_aux","=",1]]
        filtro3 = ["&",["x_studio_periodos","=",False],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]

        # DESARGAR LAS TABLAS
        f1 = self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk,ret_=True)
        f2 = self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk,ret_=True)
        f3 = self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk,ret_=True)
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
        self.resultadoBusqueda = self.quitarTrueFalse(f_final,borrar_truefalse)
        self.downloadExcel(filename,'Comunicacion masiva Corona','csv')

    def declaracion_eye(self,periodo,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """        
        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================

        modelo = 'x_unidades_vendidas'
        filtros = [('x_studio_periodo.x_name','=',periodo)]
        campos = ['x_studio_producto','x_studio_total_venta','x_studio_elementos_del_producto']
        header = ['Producto','Total venta','lista elementos'] 
        campos_fk = ['x_studio_producto']

        un_vendidas = self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk,ret_=True)
 

        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # ============================
        total_elementos = []
        for i in range(len(un_vendidas)):
            elementos = eval(un_vendidas['lista elementos'].iloc[i])
            total_elementos += elementos
            
        # =======================================
        # DESCARGAR ELEMENTOS NESDE MATERIALIDAD
        # =======================================
        modelo = 'x_materialidad'
        filtros = [('id','in',total_elementos)]
        campos = ['x_studio_producto','x_studio_descripcin','x_name','x_studio_productos_por_envase','x_studio_peso','x_studio_peso_informado',
                'x_studio_material','x_studio_caracterstica_del_material','x_studio_definir_otro_material','x_studio_caracteristica_reciclable',
                'x_studio_caracteristica_retornable','x_studio_peligrosidad','x_studio_categora','x_studio_sub_categora_material']

        header = ['Producto','Descripción','Todas las partes','Productos por envase','Peso','Peso informado',
                'Material','Característica del material','Definir otro material','Característica reciclable',
                'Caracteristica retornable','Peligrosidad','Categoría','Sub-categoría material']
        campos_fk = ['x_studio_producto']

        elementos = self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk, ret_=True,drop_id=False)
        elementos['id'] = elementos['id'].astype('int')
        
        # ===========================
        # CREAR TABLA FINAL
        # ===========================

        header1 = ['Producto'] 
        header2 = ['Descripción','Todas las partes','Productos por envase','Peso','Peso informado',
                'Material','Característica del material','Definir otro material','Característica reciclable',
                'Caracteristica retornable','Peligrosidad','Categoría','Sub-categoría material']
        header3 = ['Total venta']
        final_header = header1 + header2 + header3

        n_campos = len(final_header)
        declaracion_eye = np.zeros( (len(total_elementos),n_campos),dtype='object' )

        index_declaracion = 0
        for i in tqdm(range(len(un_vendidas))):                                      # POR CADA FILA EN VENTAS (tabla x_ventas)
            parte1 = un_vendidas[header1].iloc[i].to_numpy().reshape(1,-1)  
            parte3 = un_vendidas[header3].iloc[i].to_numpy().reshape(1,-1)
            lista_elementos = eval(un_vendidas.iloc[i]['lista elementos'])

            for elemento in lista_elementos:
                detalle_elemento = elementos[ elementos['id']==elemento ]
                detalle_elemento = detalle_elemento[header2].to_numpy().reshape(1,-1)
                row_declaracion = np.concatenate([parte1,detalle_elemento,parte3],axis=1)

                declaracion_eye[index_declaracion] = row_declaracion                # ANADE EL ELEMENTO A LA TABLA FINAL
                index_declaracion += 1

        declaracion_eye = pd.DataFrame(data=declaracion_eye,columns=final_header)
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría']=='EYE Domiciliario') | (declaracion_eye['Categoría']=='EYE No domiciliario')]

        # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================
        declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
        declaracion_eye['Peso informado']=declaracion_eye['Peso informado'].astype('float')
        declaracion_eye['Total venta']=declaracion_eye['Total venta'].astype('float')

        declaracion_eye['Peso total (gr)'] = declaracion_eye['Total venta']*declaracion_eye['Peso']
        declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['Total venta']*declaracion_eye['Peso']
        declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['Total venta']*declaracion_eye['Peso']

        # ==================================                        
        # DESCARGAR
        # ==================================
        campos_false = ['Descripción','Todas las partes','Productos por envase','Peso','Peso informado',
                'Material','Característica del material','Definir otro material','Característica reciclable',
                'Caracteristica retornable','Peligrosidad','Categoría','Sub-categoría material']
        declaracion_eye = self.quitarTrueFalse(declaracion_eye,campos_false)
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,f'Declaracion_eye Corona','xlsx')

class OdooDownloadTottus(OdooDownloadBase):
    def maestra(self,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """
        modelo = 'x_productos'
        filtros = []
        campos = ['x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_tipo_de_envase_opcional']
        header = ['SKU','Etapa','Variable de marcado','Tipo de envase (opcional)']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['Tipo de envase (opcional)']

        self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)
        self.downloadExcel(filename,'Maestra Tottus','csv')
    
    def comunicacion_masiva(self,periodo,filename=None):
        """
        Descarga la tabla de comunicacion masiva.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """
        # CODIGOS DE ETAPA DE ESTUDIOS
        no_completado_nuevo = 1
        no_completado_revision = 2
        completado = 3
        proyectado = 4
        completado_parcial = 5

        # parametros de descarga
        modelo = 'x_productos'
        campos = ['x_name','x_studio_equipo','x_studio_descripcion','x_studio_division','x_studio_proveedor','x_studio_actor_relevante','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        header = ['SKU','Equipo','Descripción','División','Proveedor','Actor relevante','Trazabilidad levantamiento','Etapa']
        campos_fk = ['x_studio_equipo','x_studio_division','x_studio_proveedor','x_studio_actor_relevante','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        borrar_truefalse = ['Actor relevante/Correo electrónico','Trazabilidad levantamiento']

        filtro1 = ["&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]
        filtro2 = ["&","&",["x_studio_periodos.x_name","=",periodo],["x_studio_stage_id","=",completado],["x_studio_variable_de_marcado","=",1]]
        filtro3 = ["&",["x_studio_periodos","=",False],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision,proyectado,completado_parcial]]]


        # DESARGAR LAS TABLAS
        f1 = self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk,ret_=True)
        f2 = self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk,ret_=True)
        f3 = self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk,ret_=True)
        f_final = pd.concat([f1,f2,f3],axis=0,ignore_index=1)


        # ADJUNTA CORREO
        actores = self.getDataFromModel('x_actores_relevantes',[],['x_name','x_studio_partner_email'],ret_=True)
        lista_correos = []                                                              
        for pm in f_final['Actor relevante']:
            try:
                email = actores[actores['x_name']==pm]['x_studio_partner_email'].values[0]
                lista_correos.append(email)
            except:
                lista_correos.append(False)
        lista_correos = pd.Series(lista_correos)
        f_final['Actor relevante/Correo electrónico'] = lista_correos
        f_final = f_final[['SKU','Equipo','Descripción','División','Proveedor','Actor relevante','Actor relevante/Correo electrónico','Trazabilidad levantamiento','Etapa']]

        self.resultadoBusqueda = self.quitarTrueFalse(f_final,borrar_truefalse)
        self.downloadExcel(filename,'Comunicacion masiva tottus','csv')
  
    def declaracion_eye(self,periodo,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """        
        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================

        modelo = 'x_unidades_vendidas'
        filtros = [('x_studio_periodo.x_name','=',periodo)]
        campos = ['x_studio_producto','x_studio_unidades_vendidas','x_studio_elementos_del_producto']
        header = ['Producto','Unidades vendidas','lista elementos'] 
        campos_fk = ['x_studio_producto']

        un_vendidas = self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk,ret_=True)

        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # ============================
        total_elementos = []
        for i in range(len(un_vendidas)):
            elementos = eval(un_vendidas['lista elementos'].iloc[i])
            total_elementos += elementos

        # =======================================
        # DESCARGAR ELEMENTOS DESDE MATERIALIDAD
        # =======================================
        modelo = 'x_materialidad'
        filtros = [('id','in',total_elementos)]
        campos = ['x_studio_descripcion','x_name','x_studio_productos_por_envase','x_studio_peso','x_studio_peso_informado','x_studio_material','x_studio_caracteristica_material',
                'x_studio_definir_otro_opcional','x_studio_caracterstica_retornable','x_studio_caracterstica_reciclable','x_studio_peligrosidad',
                'x_studio_categoria_elemento','x_studio_sub_categoria_material','x_studio_tipo_de_parte']

        header = ['Descripcion','Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                'Composición material','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',
                'Sub-categoría material','Tipo de parte']
        campos_fk = ['x_studio_material']

        elementos = self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk, ret_=True,drop_id=False)
        elementos['id'] = elementos['id'].astype('int')

        # ===========================
        # CREAR TABLA FINAL
        # ===========================
        header1 = ['Producto']
        header2 = ['Descripcion','Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                'Composición material','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',
                'Sub-categoría material','Tipo de parte']
        header3 = ['Unidades vendidas']
        final_header = header1 + header2 + header3

        n_campos = len(final_header)
        declaracion_eye = np.zeros( (len(total_elementos),n_campos),dtype='object' )

        index_declaracion = 0
        for i in tqdm(range(len(un_vendidas))):  
                producto = un_vendidas.iloc[i]['Producto']    
                lista_elementos = eval(un_vendidas.iloc[i]['lista elementos'])
                
                parte1 = np.array([producto]).reshape(-1,1)                            # header1 pero escrito de otra forma
                parte3 = un_vendidas[header3].iloc[i].to_numpy().reshape(1,-1)
            
                for elemento in lista_elementos:
                    detalle_elemento = elementos[ elementos['id']==elemento ]
                    detalle_elemento = detalle_elemento[header2].to_numpy().reshape(1,-1)
                    row_declaracion = np.concatenate([parte1,detalle_elemento,parte3],axis=1)

                    declaracion_eye[index_declaracion] = row_declaracion                # ANADE EL ELEMENTO A LA TABLA FINAL
                    index_declaracion += 1

        declaracion_eye = pd.DataFrame(data=declaracion_eye,columns=final_header)
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría elemento']=='EYE Domiciliario') | (declaracion_eye['Categoría elemento']=='EYE No domiciliario')]
        declaracion_eye = declaracion_eye.replace('False','')

         # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================

        declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
        declaracion_eye['Unidades vendidas']=declaracion_eye['Unidades vendidas'].astype('float')

        declaracion_eye['Peso total (gr)'] = declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']

        # ==================================                        
        # DESCARGAR
        # ==================================
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,'Declaracion_eye Tottus','xlsx')

class OdooDownloadDimerc(OdooDownloadBase):
   
    def maestra(self,unidad_negocio,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """
        if unidad_negocio not in ['DIMERC','PRONOBEL','DIMEIGGS']:
            raise Exception('La unidad de negocios debe ser DIMERC, PRONOBEL o DIMEIGGS')

        modelo = 'x_productos'
        filtros = [('x_studio_unidades_de_negocio','=',unidad_negocio)]
        campos = ['x_name','x_studio_stage_id','x_studio_levantamiento_asalvo']
        header = ['SKU','Etapa','Levantamiento ASALVO']
        campos_fk = ['x_studio_stage_id']

        self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk)
        self.downloadExcel(filename,f'Maestra Dimerc-{unidad_negocio}','csv')

    def comunicacion_masiva(self,periodo,unidad_negocio,filename=None):
        """
        Descarga la tabla de comunicacion masiva.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """
        if unidad_negocio not in ['DIMERC','PRONOBEL','DIMEIGGS']:
            raise Exception('La unidad de negocio debe ser DIMERC, PRONOBEL o DIMEIGGS')

        no_completado_nuevo = 1
        no_completado_revision = 2
        completado = 3

        filtro1 = ["&","&",["x_studio_periodo.x_name","=",periodo],["x_studio_unidades_de_negocio","=",unidad_negocio],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision]]]
        filtro2 = ["&","&","&",["x_studio_periodo.x_name","=",periodo],["x_studio_unidades_de_negocio","=",unidad_negocio],["x_studio_stage_id","=",completado],["x_studio_levantamiento_asalvo","=",1]]
        filtro3 = ["&","&",["x_studio_periodo","=",False],["x_studio_unidades_de_negocio","=",unidad_negocio],["x_studio_stage_id",'in',[no_completado_nuevo,no_completado_revision]]]

        campos = ['x_name','x_studio_equipo','x_studio_descripcion','x_studio_linea','x_studio_proveedor','x_studio_ean',
                  'x_studio_actor_relevante','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        modelo = 'x_productos'
        header = ['SKU','Equipo','Descripción','Línea','Proveedor','EAN','Actor relevante','Trazabilidad levantamiento','Etapa']
        campos_fk = ['x_studio_linea','x_studio_proveedor','x_studio_actor_relevante','x_studio_trazabilidad_levantamiento','x_studio_stage_id']
        borrar_truefalse = ['Equipo','Proveedor','Trazabilidad levantamiento','EAN','Actor relevante','Actor relevante/Correo electrónico']

        # DESARGAR LAS TABLAS
        f1 = self.getDataFromModel(modelo,filtro1,campos,header=header,campos_fk=campos_fk,ret_=True)
        f2 = self.getDataFromModel(modelo,filtro2,campos,header=header,campos_fk=campos_fk,ret_=True)
        f3 = self.getDataFromModel(modelo,filtro3,campos,header=header,campos_fk=campos_fk,ret_=True)
        f_final = pd.concat([f1,f2,f3],axis=0,ignore_index=1)

        # ADJUNTA CORREO
        actores = self.getDataFromModel('x_actores_relevantes',[],['x_name','x_studio_partner_email'],ret_=True)
        lista_correos = []                                                              
        for pm in f_final['Actor relevante']:
            try:
                email = actores[actores['x_name']==pm]['x_studio_partner_email'].values[0]
                lista_correos.append(email)
            except:
                lista_correos.append(False)
        lista_correos = pd.Series(lista_correos)
        f_final['Actor relevante/Correo electrónico'] = lista_correos
        f_final = f_final[['SKU','Equipo','Descripción','Línea','Proveedor','EAN','Actor relevante','Actor relevante/Correo electrónico','Trazabilidad levantamiento','Etapa']]

        self.resultadoBusqueda = self.quitarTrueFalse(f_final,borrar_truefalse)
        self.downloadExcel(filename,f'Comunicacion masiva Dimerc-{unidad_negocio}','csv')

    def declaracion_eye(self,periodo,unidad_negocio,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """
        if unidad_negocio not in ['DIMERC','PRONOBEL','DIMEIGGS']:
            raise Exception('La unidad de negocio debe ser DIMERC, PRONOBEL o DIMEIGGS')
                
        # ===========================
        # PREPARAR HEADERS ADECUADOS
        # ===========================

        if unidad_negocio=='DIMERC':
            campos_ventas_totales = ['x_studio_unidades_vendidas_dimerc']
            header_ventas_totales = ['Unidades vendidas Dimerc']

        elif unidad_negocio=='PRONOBEL':
            campos_ventas_totales = ['x_studio_unidades_vendidas_pronobel']
            header_ventas_totales = ['Unidades Vendidas Pronobel']

        elif unidad_negocio=='DIMEIGGS':
            campos_ventas_totales = ['x_studio_unidades_vendidas_dimeiggs']
            header_ventas_totales = ['Unidades Vendidas Dimeiggs']


        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================
        modelo = 'x_unidades_vendidas'
        filtros = [('x_studio_periodo_1.x_name','=',periodo)]
        campos = ['x_studio_producto','x_studio_descripcin','x_studio_todos_los_elementos'] + campos_ventas_totales
        header = ['Producto','Producto/Descripción','lista_elementos'] + header_ventas_totales 
        campos_fk = ['x_studio_producto']

        ventas = self.getDataFromModel(modelo,filtros,campos,ret_=True,campos_fk=campos_fk, header=header)

        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # =============================
        total_elementos = []
        for i in range(len(ventas)):
            elementos = eval(ventas['lista_elementos'].iloc[i])
            total_elementos += elementos
        
        # ===========================
        # DESCARGAR DETALLE DE PARTES
        # ===========================
        modelo = 'x_materialidad'
        filtros = [("id","in",total_elementos)]
        campos = ['x_name','x_studio_productos_por_envase','x_studio_peso','x_studio_peso_informado','x_studio_material','x_studio_caracteristica_material',
                    'x_studio_definir_otro_opcional','x_studio_caracteristica_retornable','x_studio_caracterstica_reciclable','x_studio_peligrosidad',
                    'x_studio_categoria_elemento','x_studio_sub_categoria_material','x_studio_tipo_de_parte']

        header = ['Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                    'Definir otro (opcional)','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',
                    'Sub-categoría material','Tipo de parte']

        campos_fk = ['x_studio_material']

        materialidad = self.getDataFromModel(modelo,filtros,campos,ret_=True,drop_id=False,campos_fk=campos_fk,header=header)
        materialidad['id'] = pd.to_numeric(materialidad['id'], errors='coerce')

        # ===========================
        # CREAR TABLA FINAL
        # ===========================

        header_1 = ['Producto','Producto/Descripción']
        header_2 = ['Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                    'Definir otro (opcional)','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',
                    'Sub-categoría material','Tipo de parte']
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
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría elemento']=='EYE Domiciliario') | (declaracion_eye['Categoría elemento']=='EYE No domiciliario')]
        declaracion_eye = declaracion_eye.replace('False','')

        # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================

        if unidad_negocio=='DIMERC':        campo_vendidas = 'Unidades vendidas Dimerc'
        elif unidad_negocio=='PRONOBEL':    campo_vendidas = 'Unidades Vendidas Pronobel'
        elif unidad_negocio=='DIMEIGGS':    campo_vendidas = 'Unidades Vendidas Dimeiggs'

        declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
        declaracion_eye[campo_vendidas]=declaracion_eye[campo_vendidas].astype('float')
        declaracion_eye['Peso total (gr)'] = declaracion_eye[campo_vendidas]*declaracion_eye['Peso']
        declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye[campo_vendidas]*declaracion_eye['Peso']
        declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye[campo_vendidas]*declaracion_eye['Peso']

        # ==================================                        
        # DESCARGAR
        # ==================================
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,f'Declaracion_eye Dimerc-{unidad_negocio}','xlsx')

class OdooDownloadIansa(OdooDownloadBase):

    def maestra(self,unidad_negocio,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """
        modelo = 'x_productos'
        filtros = [('x_studio_razn_social','=',unidad_negocio)]
        campos = ['x_name','x_studio_stage_id']
        header = ['SKU','Etapa']
        campos_fk = ['x_studio_stage_id']

        self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk)
        self.downloadExcel(filename,f'Maestra Iansa-{unidad_negocio}','csv')

    def declaracion_eye(self,periodo,unidad_negocio,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """        

        if unidad_negocio not in ["Agrocomercial IANSA", "IANSA Alimentos", "LDA SPA"]:
            raise Exception('La unidad de negocio debe ser "Agrocomercial IANSA", "IANSA Alimentos" o "LDA SPA"')

        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================
        modelo = 'x_unidades_vendidas'
        filtros = ["&",('x_studio_periodo.x_name','=',periodo),('x_studio_razn_social','=',unidad_negocio)]
        campos = ['x_studio_producto','x_studio_descripcin','x_studio_todos_los_elementos','x_studio_total_unidades_vendidas']
        header = ['Producto','Producto/Descripción','lista_elementos','TOTAL Unidades Vendidas'] 
        campos_fk = ['x_studio_producto']

        ventas = self.getDataFromModel(modelo,filtros,campos,ret_=True,campos_fk=campos_fk, header=header)

        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # =============================
        total_elementos = []
        for i in range(len(ventas)):
            elementos = eval(ventas['lista_elementos'].iloc[i])
            total_elementos += elementos

        # ===========================
        # DESCARGAR DETALLE DE PARTES
        # ===========================
        modelo = 'x_materialidad'
        filtros = [("id","in",total_elementos)]
        campos = ['x_name','x_studio_productos_por_envase','x_studio_peso','x_studio_peso_informado','x_studio_material',	
                'x_studio_caracteristica_material','x_studio_definir_otro_opcional','x_studio_caracterstica_retornable','x_studio_caracterstica_reciclable',
                'x_studio_peligrosidad','x_studio_categoria_elemento','x_studio_sub_categoria_material','x_studio_tipo_de_parte']

        header = ['Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                'Definir otro (opcional)','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',	
                'Sub-categoría material','Tipo de parte']

        campos_fk = ['x_studio_material']

        materialidad = self.getDataFromModel(modelo,filtros,campos,ret_=True,drop_id=False,campos_fk=campos_fk,header=header)
        materialidad['id'] = pd.to_numeric(materialidad['id'], errors='coerce')

        # ===========================
        # CREAR TABLA FINAL
        # ===========================

        header_1 = ['Producto','Producto/Descripción']
        header_2 = ['Elemento del producto','Productos por envase','Peso','Peso informado','Material','Característica del material',
                'Definir otro (opcional)','Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento',	
                'Sub-categoría material','Tipo de parte']
        header_3 = ['TOTAL Unidades Vendidas']
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
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría elemento']=='EYE Domiciliario') | (declaracion_eye['Categoría elemento']=='EYE No domiciliario')]
        declaracion_eye = declaracion_eye.replace('False','')

        # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================

        declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
        declaracion_eye['TOTAL Unidades Vendidas']=declaracion_eye['TOTAL Unidades Vendidas'].astype('float')
        declaracion_eye['Peso total (gr)'] = declaracion_eye['TOTAL Unidades Vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['TOTAL Unidades Vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['TOTAL Unidades Vendidas']*declaracion_eye['Peso']

        # ==================================                        
        # DESCARGAR
        # ==================================
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,f'Declaracion_eye Iansa-{unidad_negocio}','xlsx')

class OdooDownloadLuccetti(OdooDownloadBase):
    def maestra(self,filename=None):
        """
        Descarga tabla maestra.

        Parametros: 
        - unidad_negocio: Filtro Nombre de la unidad de negocio a descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto
         
        Returns:
        - Ninguno: Ninguno. Se genera un archivo en el disco duro.
        """
        modelo = 'x_productos'
        filtros = []
        campos = ['x_name','x_studio_stage_id']
        header = ['Name','Etapa']
        campos_fk = ['x_studio_stage_id']
        borrar_truefalse = ['Name']

        self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk)
        self.resultadoBusqueda = self.quitarTrueFalse(self.resultadoBusqueda,borrar_truefalse)

        self.downloadExcel(filename,f'Maestra Luccetti','csv')

    def declaracion_eye(self,periodo,filename=None):
        """
        Descarga la tabla declaracion eye.

        Parametros:
        - anho: Filtro del Periodo que se quiere descargar
        - filemane: Opcional. Nombre del archivo. Si no es especificado se utiliza nombre por defecto

        Returns:
        - Ninguno: La funcion genera un archivo csv denominado Comunicacion Masiva.csv
        """
        # =========================
        # DESCARGAR TABLA DE VENTAS
        # =========================

        modelo = 'x_unidades_vendidas'
        filtros = [('x_studio_periodo.x_name','=',periodo)]
        campos = ['x_studio_producto','x_studio_descripcin','x_studio_unidades_vendidas','x_studio_todos_los_elementos']
        header = ['Producto','Producto/Descripción','Unidades vendidas','lista elementos'] 
        campos_fk = ['x_studio_producto']

        un_vendidas = self.getDataFromModel(modelo,filtros,campos,header,campos_fk=campos_fk,ret_=True)

        # =============================
        # CONTAR ELEMENTOS A DESCARGAR
        # ============================
        total_elementos = []
        for i in range(len(un_vendidas)):
            elementos = eval(un_vendidas['lista elementos'].iloc[i])
            total_elementos += elementos
        
        # =======================================
        # DESCARGAR ELEMENTOS NESDE MATERIALIDAD
        # =======================================
        modelo = 'x_materialidad'
        filtros = [('id','in',total_elementos)]
        campos = ['x_name','x_studio_cantidades','x_studio_peso','x_studio_peso_informado','x_studio_material','x_studio_caracteristica_material','x_studio_definir_otro_opcional',
                'x_studio_caracterstica_retornable','x_studio_caracterstica_reciclable','x_studio_peligrosidad','x_studio_categoria_elemento',
                'x_studio_sub_categoria_material','x_studio_tipo_de_parte']

        header = ['Elemento del producto','Cantidades','Peso','Peso informado','Material','Característica del material','Composición material',
                'Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento','Sub-categoría material','Tipo de parte']

        campos_fk = ['x_studio_producto','x_studio_material']

        elementos = self.getDataFromModel(modelo,filtros,campos,header=header,campos_fk=campos_fk, ret_=True,drop_id=False)
        elementos['id'] = elementos['id'].astype('int')

        # ===========================
        # CREAR TABLA FINAL
        # ===========================

        header1 = ['Producto','Producto/Descripción'] 
        header2 = ['Elemento del producto','Cantidades','Peso','Peso informado','Material','Característica del material','Composición material',
                'Característica retornable','Característica reciclable','Peligrosidad','Categoría elemento','Sub-categoría material','Tipo de parte']
        header3 = ['Unidades vendidas']
        final_header = header1 + header2 + header3

        n_campos = len(final_header)
        declaracion_eye = np.zeros( (len(total_elementos),n_campos),dtype='object' )

        index_declaracion = 0
        for i in tqdm(range(len(un_vendidas))):                                      # POR CADA FILA EN VENTAS (tabla x_ventas)
            parte1 = un_vendidas[header1].iloc[i].to_numpy().reshape(1,-1)  
            parte3 = un_vendidas[header3].iloc[i].to_numpy().reshape(1,-1)
            lista_elementos = eval(un_vendidas.iloc[i]['lista elementos'])

            for elemento in lista_elementos:
                detalle_elemento = elementos[ elementos['id']==elemento ]
                detalle_elemento = detalle_elemento[header2].to_numpy().reshape(1,-1)
                row_declaracion = np.concatenate([parte1,detalle_elemento,parte3],axis=1)

                declaracion_eye[index_declaracion] = row_declaracion                # ANADE EL ELEMENTO A LA TABLA FINAL
                index_declaracion += 1

        declaracion_eye = pd.DataFrame(data=declaracion_eye,columns=final_header)
        declaracion_eye = declaracion_eye[(declaracion_eye['Categoría elemento']=='EYE Domiciliario') | (declaracion_eye['Categoría elemento']=='EYE No domiciliario')]


        # ==================================                        
        # CALCULO DE PESO*UNIDADES VENDIDAS
        # ==================================
        declaracion_eye['Peso']=declaracion_eye['Peso'].astype('float')
        declaracion_eye['Peso informado']=declaracion_eye['Peso informado'].astype('float')
        declaracion_eye['Unidades vendidas']=declaracion_eye['Unidades vendidas'].astype('float')

        declaracion_eye['Peso total (gr)'] = declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (kg)'] = 1e-3*declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']
        declaracion_eye['Peso total (ton)'] = 1e-6*declaracion_eye['Unidades vendidas']*declaracion_eye['Peso']

        # ==================================                        
        # DESCARGAR
        # ==================================
        campos_false = ['Producto/Descripción','Elemento del producto','Peso','Peso informado',
                'Material','Característica del material','Composición material','Característica reciclable',
                'Característica retornable','Peligrosidad','Sub-categoría material']
        declaracion_eye = self.quitarTrueFalse(declaracion_eye,campos_false)
        self.resultadoBusqueda = declaracion_eye
        self.downloadExcel(filename,f'Declaracion_eye Corona','xlsx')

