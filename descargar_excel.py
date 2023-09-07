from OddoDownload import OddoDownload

# ==============================================
# ============= GENERAR CONEXION ===============
# ==============================================
conn_params = {
    'ODOO_USERNAME' : 'acceso@asalvo.cl',
    'ODOO_PASSWORD' : '1234',
    'ODOO_HOSTNAME' : 'sinergia-lab-cencorep-ambiente-test-9601418.dev.odoo.com',
    'ODOO_DATABASE' : 'sinergia-lab-cencorep-ambiente-test-9601418'
}
odoo = OddoDownload(conn_params)


# ==============================================
# ========== DESCARGAR MODELOS =================
# ==============================================

# ======================= DESCARGA PERSONALIZADA
# modelo = 'x_productos'
# filtros = [('x_studio_unidades_de_negocio','=','SMK')]
# campos = ['x_studio_sku_unidad_de_negocio','x_name','x_studio_stage_id','x_studio_variable_de_marcado','x_studio_candidato_a_analisis_fisico']
# header = ['SKU unidad negocio','SKU','Etapa','EVA','Analisis fisivo']
# odoo.getDataFromModel(modelo,filtros,campos,header)

# ======================= DESCARGA PREDEFINIDA: DESCOMENTAR LA LINEA CORRESPONDIENTE
odoo.maestra('SMK')
# odoo.otroModelo1('Parametros')
# odoo.otroModelo2('Parametros')
# odoo.otroModelo3('Parametros')
print(len(odoo.resultadoBusqueda))
odoo.downloadExcel('nombre')
