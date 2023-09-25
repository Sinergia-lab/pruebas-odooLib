from OddoDownload import OdooDownloadCenco,OdooDownloadCorona,OdooDownloadTottus,OdooDownloadDimerc,OdooDownloadIansa
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

conn_params_dimerc = {
    'ODOO_USERNAME' : 'andres.romo@sinergiaindustrias.cl',
    'ODOO_PASSWORD' : 'clavefalsa',
    'ODOO_HOSTNAME' : 'rep-dimerc-test.odoo.com',
    'ODOO_DATABASE' : 'rep-dimerc-test'
}

conn_params_iansa = {
    'ODOO_USERNAME' : 'andres.romo@sinergiaindustrias.cl',
    'ODOO_PASSWORD' : 'clavefalsa',
    'ODOO_HOSTNAME' : 'rep-iansa-test.odoo.com',
    'ODOO_DATABASE' : 'rep-iansa-test'
}

odoo_cenco = OdooDownloadCenco(conn_params_cenco)
odoo_corona = OdooDownloadCorona(conn_params_corona)
odoo_tottus = OdooDownloadTottus(conn_params_tottus)
odoo_dimerc = OdooDownloadDimerc(conn_params_dimerc)
odoo_iansa = OdooDownloadIansa(conn_params_iansa)

# ==============================================
# ========== DESCARGAR MODELOS =================
# ==============================================

# ======================= DESCARGA PREDEFINIDA: DESCOMENTAR LA LINEA CORRESPONDIENTE
# odoo_cenco.maestra('SMK')                     # JUMBO SISA MDH TXD
# odoo_cenco.comunicacion_masiva(2023,'SMK') 
# odoo_cenco.declaracion_eye('TXD',2023) 


# odoo_corona.maestra()
# odoo_corona.maestra_homologos()
# odoo_corona.comunicacion_masiva(2023)
# odoo_corona.declaracion_eye(2023)

# odoo_tottus.maestra()
# odoo_tottus.comunicacion_masiva(2023)
# odoo_tottus.declaracion_eye(2023)

# odoo_dimerc.maestra('DIMERC')                           # DIMERC, PRONOBEL o DIMEIGGS
# odoo_dimerc.comunicacion_masiva(2023,'PRONOBEL')
# odoo_dimerc.declaracion_eye(2023,'PRONOBEL')

odoo_iansa.maestra('LDA SPA')                              # "Agrocomercial IANSA", "IANSA Alimentos", "LDA SPA"

# ==============================================
# ========= GENERAR ENTREGABLE EYE =============
# ==============================================

# entregable_declaracion_eye('JUMBO') # TXD,MDH,JUMBO,SISA,CORONA,TOTTUS