from xml.dom import minidom
from flask import Flask, jsonify, Response, make_response, request, send_file
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pandas as pd
import paramiko
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flasgger import Swagger
import xml.etree.ElementTree as ET
import gspread
from google.oauth2 import service_account
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token, create_refresh_token,
    get_jwt_identity, set_access_cookies,
    set_refresh_cookies, unset_jwt_cookies
)

app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
app.config["JWT_ACCESS_COOKIE_NAME"] = "access_token_cookie"
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=10)
app.config["JWT_COOKIE_CSRF_PROTECT"] = True

swagger = Swagger(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["60 per day", "30 per hour"],
    storage_uri="memory://",
)

load_dotenv()
app.config['SWAGGER'] = {
    'title': 'BORNOS API',
    'uiversion': 3
}

hostname = os.getenv('HOSTNAME_FTP')
username = os.getenv('USER_FTP')
password = os.getenv('PASSWORD_FTP')
path = os.getenv('PATH_FTP')
SCOPES = [os.getenv('SCOPES')]
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

credent = service_account.Credentials.from_service_account_file(filename="credentials.json", scopes=SCOPES)
client = gspread.authorize(credent)
document = client.open_by_key(SPREADSHEET_ID)

jwt = JWTManager(app)

def get_purcharses_server():   
  try:
      ssh_conn = paramiko.Transport((hostname, 22))
      ssh_conn.connect(username=username, password=password)
      sftp = paramiko.SFTPClient.from_transport(ssh_conn)

      files = sftp.listdir_attr(path)
      list_df = []

      for file in files:
        date_file = datetime.fromtimestamp(file.st_mtime, tz=timezone.utc).strftime('%d-%m-%Y')
        if file.filename.lower().endswith("ord_compra.csv"):
          dic = {"filename": file.filename, "date": date_file}
          with sftp.open(path + dic["filename"], "r") as ftp_file:
            ftp_file.prefetch()
            df = pd.read_csv(ftp_file, sep=",", encoding_errors="ignore")
            df["Orden Compra"] = df["Orden Compra"].astype(str).str.rstrip('.0')
            df["Ean/Upc"] = df["Ean/Upc"].astype(str).str.rstrip('.0')
            df["Fecha"] = dic["date"]
            df["Prefijo"] = file.filename[:2]
            #df[~df['Orden Compra'].isin(orders_set)]
            list_df.append(df)
      sftp.close()
      ssh_conn.close()
      
      full_df = pd.concat(list_df, ignore_index=True)

      #Esto es para calcular el total de unidades, ya que las columnas varian entre cadenas
      full_df.loc[full_df['Prefijo'] == "03", 'Cantidad'] = full_df["Paq X Empaque"] * full_df["Cantidad"]
      full_df.loc[full_df['Prefijo'] == "22", 'Cantidad'] = full_df["Cantidad"] * full_df["Piezas X Emp"]
      full_df.loc[full_df['Prefijo'].isin(["04", "05", "09"]), 'Cantidad'] = full_df['Cantidad']

      full_df.loc[full_df['Prefijo'].isin(["03", "04", "05"]), 'Precio_Unitario'] = full_df['Costo']
      full_df.loc[full_df['Prefijo'].isin(["09", "22"]), 'Precio_Unitario'] = full_df["Costo Uni"] / full_df["Empaque"]
      #full_df.loc[full_df['Prefijo'] == "22", 'Precio_Unitario'] = full_df["Costo Uni"] / full_df["Empaque"]
      #full_df.loc[full_df['Prefijo'].isin(["04", "05"]), 'Precio_Unitario'] = full_df['Costo']

      full_df.rename(columns={"Cadena": "Cliente", "Orden Compra": "Orden_Compra"}, inplace=True)
      full_df["Ax_RecId"] = pd.NA #Por default, este valor será vacio

      sheet = document.worksheet("processed_purcharses")
      data = sheet.get_all_values()
      headers = data.pop(0)

      df_processed = pd.DataFrame(data, columns=headers)
      df_processed["Orden_Compra"] = df_processed["Orden_Compra"].astype(str)
      df_processed ["Ax_RecId"] = df_processed["Ax_RecId"].astype(str)

      df = pd.merge(full_df[["Orden_Compra", "Cliente", "Ean/Upc", "Precio_Unitario", "Cantidad", "Fecha", "Prefijo"]], df_processed[["Orden_Compra", "Ax_RecId"]], on='Orden_Compra', how='left')  
      
      df["Unidad"] = "PIEZA"
      df["Tamaño"] = df["Ean/Upc"].apply(lambda x: 0.750 if x != "7501370900226" else 1)
      df["Grupo de impuestos por venta de articulos"] = "BEB"

      df["Orden_Compra"] = df["Orden_Compra"].astype(str).str.rstrip('.0')
      df["Ean/Upc"] = df["Ean/Upc"].astype(str)

      sheet = document.worksheet("prices")
      data = sheet.get_all_values()
      headers = data.pop(0)

      base = pd.DataFrame(data, columns=headers)
      #base.rename(columns={"AXAPTA": "Articulo", "Costo Unitario 2024":"Precio Unitario"}, inplace=True)
      base["Cliente_UPC"] = base["Prefijo"].astype(str) + base["UPC"].astype(str)
      df["Cliente_UPC"] = df["Prefijo"] + df["Ean/Upc"]
            
      df_result = pd.merge(df, base[['Cliente_UPC', "Precio Unitario","IEPS", "Articulo"]], on='Cliente_UPC', how='left')
      #df_result['IEPS'] = df_result['IEPS'].astype(int)
      df_result["Grupo de impuestos sobre las ventas"] = "IEPS" + (df_result['IEPS']).astype(str)
      df_result["Precio Unitario"] = round(df_result["Precio Unitario"], ndigits=2)

      df_result = df_result.loc[:, ['Orden_Compra', 'Cliente', 'Articulo', 'Cantidad', 'Unidad', "Precio Unitario", 'Tamaño', 'Grupo de impuestos sobre las ventas', 'Grupo de impuestos por venta de articulos', "Prefijo", "Fecha", "Ax_RecId"]]
      df_result.columns = df_result.columns.str.replace(' ', '_')
      return df_result
  except Exception as e:
    return e

def last_filled_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return len(str_list)

@app.route("/login", methods=["POST"])
def login():
    """Endpoint to login
    Endpoint para logearse
    ---
    parameters:
      - name: body
        in: body
        required: true
        type: object
        required:
          - username
          - password
        properties:
          username:
            type: string
            default: "UNIQUE_VALUE"
          password:
            type: string
            default: "1"
    responses:
      200:
        description: The product inserted in the database
    """
    content = request.get_json()
    if content["username"] != os.getenv("USER_API") or content["password"] != os.getenv("PASSWORD_API"):
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=content["username"])

    # Crear la respuesta y establecer la cookie
    response = make_response(jsonify({"msg": "Login successful"}))
    set_access_cookies(response, access_token)

    return response

@app.route('/logout', methods=['POST'])
def logout():
    """Endpoint to logout
    Endpoint para invalidar el token de autentificación
    ---
    responses:
      200:
        description: logout
    """
    resp = jsonify({'logout': True})
    unset_jwt_cookies(resp)
    return resp, 200

@app.route('/get_purcharses_day/<date>', methods=['GET'])
#@jwt_required()
def get_purcharses_day(date):
    """
    Endpoint to get all the purchases of a specific date in json format.
    Endpoint para obtener las ordenes de compra de un día específico aún no procesadas (sin Ax_RecId) en formato json.
    date format: year-month-day
    ---
    parameters:
      - name: date
        in: path
        type: string
        required: true
    responses:
      200:
        description: A list of purcharses from a specific date
    """

    """
    Comentario
    ---
    responses:
      200:
        description: All the purcharses
    """
    df = get_purcharses_server()
    df = df[(df['Fecha'] == date)]
    df = df[df['Ax_RecId'].isna()]
    df = df.drop(['Ax_RecId'], axis=1)

    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/post_purcharses', methods=['POST'])
#@jwt_required()
def post_purcharses():
    """Endpoint to proccess a purchase order as a sales order. The provided Ax_RecId is added to the server files.
    Endpoint para procesar una order de compra como orden de venta. El Ax_RecId dado se registra en los archivos del servidor
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          id: sale_order
          type: array
          items:
            type: object
            required:
              - Orden_Compra
              - Ax_RecId
            properties:
              Orden_Compra:
                type: string
                default: "UNIQUE_VALUE"
              Ax_RecId:
                type: string
                default: "1"
    responses:
      200:
        description: The product inserted in the database
    """

    try:
      content = request.get_json()
      #df = get_purcharses_server()
      df = get_purcharses_server()
      orders = []
      date_process = datetime.today().strftime("%d-%m-%Y")
      time_process = datetime.now().strftime("%H:%M")
      for sale_order in content:
        elem = [sale_order["Orden_Compra"], sale_order['Ax_RecId'], date_process, time_process]
        orders.append(elem)

      sheet_processed = document.worksheet("processed_purcharses")
      sheet_processed.append_rows(orders)

      sheet = document.worksheet("processed_purcharses_details")
      for sale_order in content:
        id_order = sale_order["Orden_Compra"]
        df_filtered = df[(df['Orden_Compra'] == id_order)]
        df_filtered["Ax_RecId"] = sale_order['Ax_RecId']
        df_filtered["Fecha"] = date_process
        df_filtered = df_filtered[["Orden_Compra", "Cliente", "Articulo", "Cantidad", "Fecha", "Ax_RecId"]]
        sheet.append_rows(df_filtered.values.tolist())

      return "Success"
    except Exception as e:
      return "Error " + str(e)
    
# Endpoint para retornar el XML
@app.route('/get_xml_purcharses/<date>')
#@jwt_required()
def get_xml_purcharses(date):
    """
    Endpoint to get all the specific date unprocessed purchases in xml format.
    Endpoint para obtener las ordenes de compra de un día específico aún no procesadas en formato xml.
    format: day-month-year, example: 02-12-2024
    ---
    parameters:
      - name: date
        in: path
        type: string
        required: true
    responses:
      200:
        description: All the purcharses
    """
    df = get_purcharses_server()
    df = df[(df['Fecha'] == date)]
    df = df[df['Ax_RecId'].isna()]

    df = df.drop(['Fecha', 'Ax_RecId'], axis=1)

    orders_tag = ET.Element("Ordenes")
    orders_numbers = set(df["Orden_Compra"])

    for order_number in orders_numbers:
        order_tag = ET.SubElement(orders_tag, "Orden")

        list_ord = df.query(f"Orden_Compra == '{order_number}'")
        try:
          prefix_client = list_ord["Prefijo"].iloc[0]
          sheet_accounts = document.worksheet("accounts")
          data = sheet_accounts.get_all_values()
          headers = data.pop(0)
          df_accounts = pd.DataFrame(data, columns=headers)
          id_account = str(df_accounts.query(f"Prefijo == '{prefix_client}'")["Cuenta"].iloc[0])
        except:
          id_account = "Error"

        df_header = pd.DataFrame({
        'Orden_Compra': [order_number],
        "Cliente": [id_account],
        'Sitio': ["Vitinico"], 
        'Almacen': ["IZTAPALAPA"], 
        'Departamento': ["deo"], 
        'Centro_de_costo': ["VEVEVEVL1"],
        'Reporte': [""], 
        'Tipo_de_Gasto': ["OPROP"], 
        'Financiera': ["TRANOF"], 
        'Proposito': ["DIS"], 
        'Tesoreria': ["00004000"]
        })

        root_df_header = ET.fromstring(df_header.to_xml(index=False))
        for row in root_df_header:
            header_tag = ET.SubElement(order_tag, "Cabecera")
            for child in row:
                child_element = ET.SubElement(header_tag, child.tag)
                child_element.text = child.text

        concepts_tag= ET.SubElement(header_tag, "Conceptos")
        list_ord = list_ord.drop(['Orden_Compra','Cliente'], axis=1)

        root_df_concepts = ET.fromstring(list_ord.to_xml(index=False))

        for row in root_df_concepts:
            concept_tag = ET.SubElement(concepts_tag, "Concepto")
            for child in row:
                child_element = ET.SubElement(concept_tag, child.tag)
                child_element.text = child.text
                
    xml_str = ET.tostring(orders_tag, encoding="unicode", method="xml")
    xml_str_formatted = minidom.parseString(xml_str).toprettyxml(indent="  ")
    # Retornar el XML con el tipo MIME adecuado
    return Response(xml_str_formatted, mimetype='application/xml', status=200)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
