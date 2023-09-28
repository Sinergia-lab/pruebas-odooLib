import boto3
import botocore
from botocore.exceptions import ClientError

from pandas import DataFrame
import datetime
import os



class Aws:
    def __init__(self):
        access = '' # Las credenciales las tengo en el escritorio
        secret = ''

        self.s3_resource=boto3.resource('s3',aws_access_key_id=access,aws_secret_access_key=secret)
    def obtenerBucket(self,nombreBucket):
        bucket=self.s3_resource.Bucket(nombreBucket)
        if bucket:
            return bucket
    def getListadoArchivos(self,bucket, prefix=None):

        objetos=bucket.objects.filter(Prefix=prefix)

        print(objetos)
        for obj in objetos:
            print(obj.key)
        return objetos
    def descargarArchivo(self,bucket,keyObjeto,rutaDestinoArchivo=None):
        print('intentando iniciar la descarga de '+keyObjeto)
        
        '''
        if self.obtenerBucket(nombreBucket).download_file(Key=nombreArchivo,Filename="/"+nombreArchivo):
            print('...Documento '+nombreArchivo+' descargado en '+rutaDestinoArchivo)
        else:
            print('...Descarga fallida')
        '''
        try:
            '''with open('filename', 'wb') as data:
                bucket.download_fileobj(keyObjeto,rutaDestinoArchivo, data)'''
            bucket.download_file(keyObjeto, rutaDestinoArchivo)
            print('...Documento '+keyObjeto+' descargado')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("El objeto no existe")
                print(e)
            else:
                raise

    def getNombreObjetoDB(self,obj,unidadNeg):
        if unidadNeg=='SMK':
            if obj.key.find('_LeyRep_Producto_SM')>0:
                return 'producto_SM'
            elif obj.key.find('_LeyRep_Insumo_SM')>0:
                return 'insumo_SM'
            elif obj.key.find('_LeyRep_Venta_SM')>0:
                return 'venta_SM'
            elif obj.key.find('_CATEGORIA_LEYREPCL_SMK')>0:
                return 'categoria_SM'
            elif obj.key.find('_PROVEEDOR_LEYREPCL_SMK')>0:
                return 'proveedor_SM'
            else:
                return 'Nombre_archivo_no_reconocido'
        elif unidadNeg=='MDH':
            if obj.key.find('_LeyRep_Producto_MDH')>0:
                return 'producto_MDH'
            elif obj.key.find('_LeyRep_Insumo_MDH')>0:
                return 'insumo_MDH'
            elif obj.key.find('_LeyRep_Venta_MDH')>0:
                return 'venta_MDH'
            elif obj.key.find('_CATEGORIA_LEYREPCL_MDH')>0:
                return 'categoria_MDH'
            elif obj.key.find('_PROVEEDOR_LEYREPCL_MDH')>0:
                return 'proveedor_MDH'
            else:
                return 'Nombre_archivo_no_reconocido'
        elif unidadNeg=='TXD':
            if obj.key.find('_LeyRep_TXD_Venta')>0:
                return 'producto_TXD'
            elif obj.key.find('_LeyRep_Insumo_TXD')>0:
                return 'insumo_TXD'
            else:
                return 'Nombre_archivo_no_reconocido'

        elif unidadNeg=='CORPORATIVO':

            valor =obj.key.find('SIG_SISA_SMK(NO_COMPLETADO)')
            if obj.key.find('SIG_SISA_SMK(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_MDH(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_TXD(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_SISA_SMK(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_MDH(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_TXD(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            
            else:
                return 'Nombre_archivo_no_reconocido'
        else:
            return 'Nombre_archivo_no_reconocido'
    
    def cambiarDocumentoFolder(self,nombreBucket,keyObj,rutaDestino,unidadNegocio):
        copy_source = {
            'Bucket': nombreBucket,
            'Key': keyObj
        }
        nombre=keyObj.replace(unidadNegocio+'/','')
        self.s3_resource.meta.client.copy(copy_source, nombreBucket, rutaDestino+nombre)
        self.s3_resource.meta.client.delete_object(
            Bucket=nombreBucket,
            Key=keyObj
        )
    def eliminarDocumentoFolder(self,nombreBucket,keyObj):
        self.s3_resource.meta.client.delete_object(
            Bucket=nombreBucket,
            Key=keyObj
        )
    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = self.s3_resource.meta.client
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    def obtenerDataFrames(self,unidadNeg,nombreBucket,tipo,modoCarga):
        bb=self.obtenerBucket(nombreBucket)
        listadoArchivos=self.getListadoArchivos(bb,unidadNeg+'/')
        dataFrames=[]
        if listadoArchivos:        
            for archivo in listadoArchivos:
                key= archivo.key
                nombreDocumento= key.replace(unidadNeg+'/','')
                if not archivo.key==unidadNeg+'/':
                    nombreobj=self.getNombreObjetoDB(archivo,unidadNeg)
                    rutaDestino='/tmp/'+nombreobj
                    if modoCarga=='cencoOdoo':
                        valor=archivo.key.find('.csv')
                        if valor!=-1:
                            self.descargarArchivo(bb,archivo.key,rutaDestino)
                            df = DataFrame(self.getNombreObjetoDB(archivo,unidadNeg),cargarDataFrame(rutaDestino,tipo),unidadNeg,archivo.key,nombreBucket)
                            dataFrames.append(df)
                    elif modoCarga=='declaracionSIG':        
                        valor=archivo.key.find('(COMPLETADO)')
                        if valor==-1:
                            self.descargarArchivo(bb,archivo.key,rutaDestino)
                            df = DataFrame(self.getNombreObjetoDB(archivo,unidadNeg),cargarDataFrame(rutaDestino,tipo),unidadNeg,archivo.key,nombreBucket)
                            dataFrames.append(df)
            return dataFrames
        return None

    def eliminarDocumentosAntiguos(self,nombeBucket,folder):
        bb=self.obtenerBucket(nombeBucket)
        prefijo=folder+'/'
        listadoArchivos=self.getListadoArchivos(bb,prefix=prefijo)
        if listadoArchivos:        
            for archivo in listadoArchivos:
                nombreSinCarpeta=archivo.key.replace(prefijo,'')
                if not nombreSinCarpeta =='':
                    ultimaActualizacionObj=archivo.last_modified.replace(tzinfo=None)
                    now=datetime.datetime.now()
                    resta=now-ultimaActualizacionObj
                    if resta.days>DAY_LIMIT:
                        print ('Eliminando archivo '+ archivo.key)

                        self.eliminarDocumentoFolder(nombeBucket,archivo.key)
