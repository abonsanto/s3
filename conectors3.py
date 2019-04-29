#!/usr/bin/env python
# -​- coding: utf-8 -​-
import config
import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from  urlparse import urlparse


class ConectorS3 (object):

    """
        Metodo : ConectorS3
            "Amazon Simple Storage Service (Amazon S3) ofrece a los desarrolladores
            y los profesionales de TI un almacenamiento de objetos seguro, duradero
            y altamente escalable. Amazon S3 es fácil de utilizar e incorpora una
            sencilla interfaz de servicios web para almacenar y recuperar cualquier
            cantidad de datos desde cualquier ubicación de la web"
        Fucionalidad: conectar al contenedor s3 Amazon de una forma sencilla
                      desde python
        Librerias requeridas : importando las libreria boto es capaz de acceder
                               desde web2py al servicio de Amazon
        parametros de entrada : claveAccesoPublica y ClaveAccesoPrivada
        parametros de salida  : nombre del contenedor creado
    """
    #claves de contenedor de prueba

    def __init__(self):
        self.claveAccesoPublica = config.archivo.json()['amazon']['claveAccesoPublica']
        self.ClaveAccesoPrivada = config.archivo.json()['amazon']['ClaveAccesoPrivada']

    def conectar(self):
        """
            Metodo : conectar
            Fucionalidad: crear la conexion con S3 Amazon
            parametros de entrada : claveAccesoPublica ("clave publica que
                                    proporciona s3 al contratar el servicio")
                                    ClaveAccesoPrivada ("clave privada que
                                    proporciona s3 al contratar el servicio")
            parametros de salida  : el objeto de conexion a s3
            Modificacion :
        """
        try:
            conectar = S3Connection(self.claveAccesoPublica,
                                    self.ClaveAccesoPrivada)
        except boto.exception.AWSConnectionError, e:
            raise IOError("Error de Conexion")
        return conectar


    def crearBucket(self, nombreContenedor=""):
        """
            Metodo : crearBucket
            Fucionalidad: crear un bucket ("contenedor") dentro de S3 Amazon
            parametros de entrada : nombreContenedor ("nombre del contenedor que
                                    se quiere crear")
            parametros de salida  : nombre del contenedor creado
            Modificacion :
        """

        conexion = self.conectar()
        try:
            contenedor = conexion.create_bucket(nombreContenedor)
        except boto.exception.S3ResponseError, e:
            raise AttributeError("Error creando el contenedor")
        else:
            resp = dict()
            resp["nombreContenedor"] = nombreContenedor
            return resp


    def listarContenedor(self):
        """
            Metodo : listarContenedor
            Fucionalidad: mostrar los bucket ("contenedor") dentro de S3 Amazon
            parametros de entrada :
            parametros de salida  : diccionario con los nombres de contenedor
                                    creados
            Modificacion :
        """
        conexion = self.conectar()
        try:
            contenedores = conexion.get_all_buckets()
        except boto.exception.S3ResponseError, e:
            raise IOError("Contenedor no encontrado")
        else:
            lista = dict()
            # contenedores = objeto que retorna get_all_buckets() debe realizarle
            #un for para poder acceder a sus posiciones y devolver un diccionario
            for key in contenedores:
                pos = len(lista)
                lista[pos] =key.name
            return lista


    def listarClaves(self, nombreContenedor=""):
        """
            Metodo : listarClaves
            Fucionalidad: mostrar las claves dentro de un bucket ("contenedor")
                          de S3 Amazon
            parametros de entrada : nombreContenedor ("nombre del contenedor que
                                    se quiere consultar")
            parametros de salida  : diccionario con las claves dentro del
                                    contenedor
            Modificacion :
        """
        conexion = self.conectar()
        contenedor = self.obtenerContenedor(conexion,nombreContenedor)
        lista = dict()
        # contenedor = objeto que retorna get_all_buckets() debe realizarle un
        #for para poder acceder a sus posiciones y devolver un diccionario
        for key in contenedor.list():
            pos = len(lista)
            lista[pos] =key.name
        return lista


    def obtenerContenedor(self, conexion, nombreContenedor):
        """
            Metodo : obtenerContenedor
            Fucionalidad: retornar un objeto de contenedor para la clase Key
            parametros de entrada : nombreContenedor ("nombre del contenedor que
                                    se quiere consultar")
            parametros de salida  : objeto bucket
            Modificacion :
        """
        try:
            contenedor = conexion.get_bucket(nombreContenedor)
        except boto.exception.S3ResponseError, e:
            raise IOError("Contenedor no encontrado")
        else:
            return contenedor


    def crearClave(self,nombreArchivo, contenedor, carpeta):
        if carpeta:
            clave = carpeta+"/"+nombreArchivo
        else:
            clave = contenedor.new_key(nombreArchivo)
        return clave


    def subirArchivo(self,nombreContenedor="",clave="",archivo="",carpeta=False):
        """
            Metodo : subirArchivo
            Fucionalidad: subir archivos al bucket ("contenedor") dentro de S3
                          Amazon
            parametros de entrada : nombreContenedor ("nombre del contenedor en
                                    donde se desee guardar")
                                    clave ("clave que se desea asignar al archivo")
                                    archivo ("archivo que se desea agregar en el
                                    contenedor")
            parametros de salida  : diccionario que contiene
                                        link del archivo cargado
                                        nombre del contenedor
                                        clave del archivo
                                        nombre del archivo
            Modificacion :
        """
        if not clave:
            raise UnboundLocalError("Clave no definida")
        if not archivo:
            raise UnboundLocalError("Archivo no definido")
        conexion = self.conectar()
        contenedor = self.obtenerContenedor(conexion,nombreContenedor)
        #Key = objeto de boto que se necesita para poder acceder a s3
        objetoClave = Key(contenedor)
        objetoClave.key = self.crearClave(clave,contenedor,carpeta)
        #nombre = archivo.split("/") #nombre = se realiza el split para poder
        #obtener el nombre del archivo de la URL
        try:
            insertar= objetoClave.set_contents_from_filename(archivo,
                                                             replace=False,
                                                             policy="public-read")
        except boto.exception.S3ResponseError, e:
            raise IOError("Directorio no encontrado")
        except IOError, e:
            raise IOError("Directorio no encontrado")
        else:
            # objetoInsertado = set_contents_from_filename() devuelve None si no
            # guarda en s3
            if insertar == None:
                raise IOError("Clave existente en el contenedor")
            else:
                #objetoClave.key = se debe definir la clave del contenedor que
                #se va obtener el URL
                objetoClave.key = self.crearClave(clave,contenedor,carpeta)
                resp = dict()
                resp["link"] = objetoClave.generate_url(0, query_auth=False,
                                                        force_http=False)
                #resp["contenedor"] = contenedor.name
                resp["key"] = objetoClave.key
                #resp["nombre_archivo"] = nombre[len(nombre)-1]
                return resp


    def leerArchivo(self, nombreContenedor="", clave="", nombreArchivo=""):
        """
            Metodo : leerArchivo
            Fucionalidad: leer archivos del bucket ("contenedor") seleccionado
                          dentro de S3 Amazon
            parametros de entrada : nombreContenedor ("nombre del contenedor en
                                    que se desea buscar informacion")
                                    clave ("clave del archivo que se busca")
                                    archivo ("archivo que se busca en el
                                             contenedor")
            parametros de salida  : diccionario que contiene
                                        link del archivo cargado
                                        nombre del contenedor
                                        clave del archivo
                                        nombre del archivo
            Modificacion :
        """
        if not clave:
            raise UnboundLocalError("Clave no definida")
        if not nombreArchivo:
            raise UnboundLocalError("Archivo no definido")
        conexion = self.conectar()
        contenedor = self.obtenerContenedor(conexion,nombreContenedor)
        #Key = objeto de boto que se necesita para poder acceder a s3
        objetoClave = Key(contenedor)
        objetoClave.key = clave
        try:
            contenido = objetoClave.get_contents_to_filename(nombreArchivo)
        except boto.exception.S3ResponseError, e:
            raise IOError("Archivo no encontrado en s3")
        except IOError, e:
            raise IOError("Archivo no encontrado en s3")
        else:
            resp = dict()
            resp["link"] = objetoClave.generate_url(0, query_auth=False,
                                                    force_http=False)
            resp["contenedor"] = contenedor.name
            resp["key"] = objetoClave.key
            return resp


    def crearCadena(self,nombreContenedor="",clave="",cadena=""):
        """
            Metodo : crearCadena
            Fucionalidad: crear una cadena ("contenedor") dentro de S3 Amazon
            parametros de entrada : nombreContenedor ("nombre del contenedor en
                                    donde se desee guardar")
                                    cadena que se desea guardar en el S3
            parametros de salida  : clave del contenedor y clave del archivo
            Modificacion :
        """
        if not clave:
            raise UnboundLocalError("Clave no definido")
        if not cadena:
            raise UnboundLocalError("Cadena no definido")
        conexion = self.conectar()
        contenedor = self.obtenerContenedor(conexion,nombreContenedor)
        objetoClave = Key(contenedor)
        objetoClave.key = contenedor.new_key(clave)
        try:
            insertar = objetoClave.set_contents_from_string(cadena,
                                                            replace=False,
                                                            policy="public-read")
        except boto.exception.S3ResponseError, e:
            raise IOError("fichero o directorio no encontrado")
        else:
            if insertar == None:
                raise IOError("Clave existente en el contenedor")
            else:
                #objetoClave.key = se debe definir la clave del contenedor que
                #se va obtener el URL
                objetoClave.key = clave
                resp = dict()
                resp["link"] = objetoClave.generate_url(0, query_auth=False,
                                                        force_http=False)
                resp["contenedor"] = contenedor.name
                resp["key"] = objetoClave.key
                return resp


    def leerCadena(self,nombreContenedor="",clave=""):
        """
            Metodo : leerCadena
            Fucionalidad: buscar una cadena dentro de S3 Amazon
            parametros de entrada : nombreContenedor : ("nombre del contenedor
                                    en donde se desee buscar")
                                    clave : que se desea buscar en el S3
            parametros de salida  : clave del contenedor y clave del archivo
            Modificacion :
        """
        if not clave:
            raise UnboundLocalError("Clave no definido")
        conexion = self.conectar()
        contenedor = self.obtenerContenedor(conexion,nombreContenedor)
        objetoClave = Key(contenedor)
        objetoClave.key = clave
        if objetoClave:
            try:
                objetoConsultado = objetoClave.get_contents_as_string()
            except boto.exception.S3ResponseError, e:
                raise IOError("fichero o directorio no encontrado")
            else:
                return objetoConsultado.decode(encoding='utf-8')


    def dividirRuta(self, s3url):
        """
            .. function::  dividirRuta(s3url)

               obtener de la ruta el nombre del contenedor y la clave

               :param s3url: cadena con la ruta del archivo en Amazon
               :returns: nombre del contenedor y la clave
            :Modificación:
        """
        parsed_url = urlparse(s3url)
        dominio = parsed_url.netloc
        bucket_name = dominio.split(".s3.amazonaws.com")[:1]
        bucket_name = str(bucket_name[0])
        if parsed_url.path[0] == '/':
            key = parsed_url.path[1:]
        else:
            key = parsed_url.path
        return (bucket_name,key)


    def obtenerClave(self, contenedor, clave, nombreContenedor=None):
        """
            .. function::  obtenerClave(contenedor, key, nombreContenedor=None)

               obtener el objeto clave del contenedor enviado

               :param contenedor: objeto de contenedor amazon
               :param clave: clave del archivo cargado en amazon
               :param nombreContenedor: nombre del contenedor de amazon
               :returns: objeto clave o False en el caso de no existir
            :Modificación:
        """
        try:
            conexion = self.conectar()
            if not nombreContenedor:
                (nombreContenedor, clave) = self.dividirRuta(clave)
            clave = contenedor.get_key(clave,validate=True)
            if clave != None:
                return clave
            else:
                return False
        except (IOError,NameError), error:
            return str(error)


    def eliminarClave(self,contenedor,clave):
        """
            .. function::  eliminarClave(contenedor, clave)

               eliminar la clave corespondiente al archivo cargado en amazon

               :param contenedor: objeto de contenedor amazon
               :param clave: clave del archivo cargado en amazon
               :returns: True o False dependiendo de la operacion realizada
            :Modificación:
        """
        if contenedor and clave:
            respuesta = contenedor.delete_key(clave)
            if respuesta.name:
                resp= self.obtenerClave(contenedor,clave)
                if resp == False:
                    return True
        return False


    def eliminarPorRuta(self, ruta):
        """
            .. function::  eliminarPorRuta(ruta)

               eliminar un archivo de amazon por medio de la ruta enviada

               :param ruta: ruta del archivo caragado en amazon
               :returns: True o False dependiendo de la operacion realizada
        """
        conexion = self.conectar()
        try:
          if not ruta:
              raise UnboundLocalError("Envie una ruta de Amazon")
          else:
              (nombreContenedor,clave)= self.dividirRuta(ruta)
              contenedor = self.obtenerContenedor(conexion,nombreContenedor)
              key = self.obtenerClave(contenedor,clave)
              if key != False:
                  return self.eliminarClave(contenedor,clave)
              else:
                  return "El archivo no existe"
        except IOError, e:
          return str(e)
