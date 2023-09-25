from OddoDownload import OdooDownloadCenco,OdooDownloadCorona,OdooDownloadTottus
from entregableEye import entregable_declaracion_eye

# ==============================================
# ============= GENERAR CONEXION ===============
# ==============================================
conn_params_cenco = {
    'ODOO_USERNAME' : 'acceso@asalvo.cl',
    'ODOO_PASSWORD' : '1234',
    'ODOO_HOSTNAME' : 'sinergia-lab-cencorep-ambiente-test-9601418.dev.odoo.com',
    'ODOO_DATABASE' : 'sinergia-lab-cencorep-ambiente-test-9601418'
}

conn_params_corona = {
    'ODOO_USERNAME' : 'andres.romo@sinergiaindustrias.cl',
    'ODOO_PASSWORD' : 'clavefalsa',
    'ODOO_HOSTNAME' : 'repcorona-test.odoo.com',
    'ODOO_DATABASE' : 'repcorona-test'
}

conn_params_tottus = {
    'ODOO_USERNAME' : 'andres.romo@sinergiaindustrias.cl',
    'ODOO_PASSWORD' : 'clavefalsa',
    'ODOO_HOSTNAME' : 'rep-tottus-test.odoo.com',
    'ODOO_DATABASE' : 'rep-tottus-test'
}


odoo_cenco = OdooDownloadCenco(conn_params_cenco)
odoo_corona = OdooDownloadCorona(conn_params_corona)
odoo_tottus = OdooDownloadTottus(conn_params_tottus)

# ==============================================
# ========== DESCARGAR MODELOS =================
# ==============================================

# ======================= DESCARGA PREDEFINIDA: DESCOMENTAR LA LINEA CORRESPONDIENTE
# odoo_cenco.maestra('SMK')
# odoo_cenco.comunicacion_masiva(2023,'SMK') #
odoo_cenco.declaracion_eye('TXD',2023) # JUMBO SISA MDH TXD


# odoo_corona.maestra()
# odoo_corona.maestra_homologos()
# odoo_corona.comunicacion_masiva(2023)
# odoo_corona.declaracion_eye(2023)

# odoo_tottus.maestra()
# odoo_tottus.comunicacion_masiva(2023)
# odoo_tottus.declaracion_eye(2023)

# ==============================================
# ========= GENERAR ENTREGABLE EYE =============
# ==============================================

# entregable_declaracion_eye('JUMBO') # TXD,MDH,JUMBO,SISA,CORONA,TOTTUS