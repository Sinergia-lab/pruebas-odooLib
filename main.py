from OddoDownload import OdooDownloadCenco,OdooDownloadCorona,OdooDownloadTottus,OdooDownloadDimerc,OdooDownloadIansa,OdooDownloadLuccetti

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

conn_params_luccetti = {
    'ODOO_USERNAME' : 'andres.romo@sinergiaindustrias.cl',
    'ODOO_PASSWORD' : 'clavefalsa',
    'ODOO_HOSTNAME' : 'sinergia-lab-tmluc-tmluctestdescarga-9848792.dev.odoo.com',
    'ODOO_DATABASE' : 'sinergia-lab-tmluc-tmluctestdescarga-9848792'
}

odoo_cenco = OdooDownloadCenco(conn_params_cenco)
odoo_corona = OdooDownloadCorona(conn_params_corona)
odoo_tottus = OdooDownloadTottus(conn_params_tottus)
odoo_dimerc = OdooDownloadDimerc(conn_params_dimerc)
odoo_iansa = OdooDownloadIansa(conn_params_iansa)
odoo_luccetti = OdooDownloadLuccetti(conn_params_luccetti)

# ==============================================
# ========== DESCARGAR MODELOS =================
# ==============================================

# ======================= DESCARGA PREDEFINIDA: DESCOMENTAR LA LINEA CORRESPONDIENTE
# odoo_cenco.maestra('TXD')                     # SMK MDH TXD
# odoo_cenco.comunicacion_masiva(2023,'TXD') 
# odoo_cenco.declaracion_eye(2023,'MDH')          # JUMBO SISA MDH TXD
# odoo_cenco.entregable_eye(2023,'JUMBO')

# odoo_corona.maestra()
# odoo_corona.comunicacion_masiva(2023)
# odoo_corona.declaracion_eye(2023)
# odoo_corona.entregable_eye(2023)

# odoo_tottus.maestra(filename='Ejemplo tottus')
# odoo_tottus.comunicacion_masiva(2023)
# odoo_tottus.declaracion_eye(2023)
# odoo_tottus.entregable_eye(2023)

# odoo_dimerc.maestra('PRONOBEL')                           # DIMERC, PRONOBEL o DIMEIGGS
# odoo_dimerc.comunicacion_masiva(2023,'PRONOBEL')
# odoo_dimerc.declaracion_eye(2023,'DIMEIGGS')
odoo_dimerc.entregable_eye(2023,'PRONOBEL')

# odoo_iansa.maestra('LDA SPA')                           # "Agrocomercial IANSA", "IANSA Alimentos", "LDA SPA"
# odoo_iansa.declaracion_eye(2023,'LDA SPA')
# odoo_iansa.entregable_eye(2023,'Agrocomercial IANSA')
# odoo_iansa.entregable_eye(2023,'IANSA Alimentos')
# odoo_iansa.entregable_eye(2023,'LDA SPA')

# odoo_luccetti.maestra()
# odoo_luccetti.declaracion_eye(2023,'god')
# odoo_luccetti.entregable_eye(2023)

# ==============================================
# ========= GENERAR ENTREGABLE EYE =============
# ==============================================

# entregable_declaracion_eye('JUMBO') # TXD,MDH,JUMBO,SISA,CORONA,TOTTUS