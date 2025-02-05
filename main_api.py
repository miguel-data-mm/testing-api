from xml.dom import minidom
from flask import Flask, jsonify, Response, request, send_file
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

#load_dotenv()
app = Flask(__name__)
swagger = Swagger(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["400 per day", "200 per hour"],
    storage_uri="memory://",
)

load_dotenv()
# Configuring Swagger
app.config['SWAGGER'] = {
    'title': 'BORNOS API',
    'uiversion': 3
}

hostname = os.getenv('HOSTNAME')
username = os.getenv('USER')
password = os.getenv('PASSWORD')
path = os.getenv('PATH_FTP')

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credent = service_account.Credentials.from_service_account_file(filename="credentials.json", scopes=SCOPES)
# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1pYh0xT3-w1RCokQZkAzM2BWXo68yrGGCr86N7SSpNIM"
client = gspread.authorize(credent)
document = client.open_by_key(SPREADSHEET_ID)

def get_purcharses_server():   
  try:
      ssh_conn = paramiko.Transport((hostname, 22))
      ssh_conn.connect(username=username, password=password)
      sftp = paramiko.SFTPClient.from_transport(ssh_conn)

      files = sftp.listdir_attr("/45801/Send/")
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
            #df[~df['Orden Compra'].isin(orders_set)]
            list_df.append(df)

      sftp.close()
      ssh_conn.close()
      
      full_df = pd.concat(list_df, ignore_index=True)
      full_df['Piezas'] = full_df['Piezas X Emp'].fillna(full_df['Paq X Empaque'])
      full_df["Cantidad"] = full_df["Cantidad"] * full_df["Piezas"]
      full_df.rename(columns={"Cadena": "Cliente", "Orden Compra": "Orden_Compra"}, inplace=True)
      full_df["Ax_RecId"] = pd.NA

      sheet = document.worksheet("processed_purcharses")
      data = sheet.get_all_values()
      headers = data.pop(0)

      df_processed = pd.DataFrame(data, columns=headers)
      df_processed["Orden_Compra"] = df_processed["Orden_Compra"].astype(str)
      df_processed ["Ax_RecId"] = df_processed["Ax_RecId"].astype(str)
      full_df = pd.merge(full_df[["Orden_Compra", "Cliente", "Ean/Upc", "Cantidad", "Fecha"]], df_processed[["Orden_Compra", "Ax_RecId"]], on='Orden_Compra', how='left')  
      return full_df
  except Exception as e:
    return e

def last_filled_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return len(str_list)

@app.route('/get_purcharses_day/<date>', methods=['GET'])
def get_purcharses_day(date):
    """
    Endpoint to get all the unprocessed purchases of a specific date in json format.
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
    df  = df[df['Ax_RecId'].isna()]
    #df = df[(df['Fecha'] == date)]

    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/post_purcharses', methods=['POST'])
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
    content = request.get_json()
    #df = get_purcharses_server()
    orders = []
    for sale_order in content:
      elem = [sale_order["Orden_Compra"], sale_order['Ax_RecId'], datetime.today().strftime("%d-%m-%Y"), datetime.now().strftime("%H:%M")]
      #{"Orden_Compra": sale_order["Orden_Compra"], 'Ax_RecId': sale_order['Ax_RecId'], "Fecha": datetime.today().strftime("%d-%m-%Y"), "Hora": datetime.now().strftime("%H:%M")}
      orders.append(elem)

    sheet_processed = document.worksheet("processed_purcharses")
    sheet_processed.append_rows(orders)

    df = get_purcharses_server()
    id_order = sale_order["Orden_Compra"]
    df = df[(df['Orden_Compra'] == id_order)]
    df = df[["Orden_Compra", "Cliente", "Ean/Upc", "Cantidad", "Fecha", "Ax_RecId"]]
    sheet = document.worksheet("processed_purcharses_details")
    sheet.append_rows(df.values.tolist())
    test = df.values.tolist()
    
    return test

# Endpoint para retornar el XML
@app.route('/get_xml_purcharses/<date>')
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
    #df = df[(df['Fecha'] == date)]
    df = df[df['Ax_RecId'].isna()]
  
    df["Unidad"] = "PIEZA"
    df["Tamaño"] = df["Ean/Upc"].apply(lambda x: 1 if x != "7501370900233" else 0.750)
    df["Grupo de impuestos por venta de articulos"] = "BEB"

    df["Orden_Compra"] = df["Orden_Compra"].astype(str).str.rstrip('.0')
    df["Ean/Upc"] = df["Ean/Upc"].astype(str).str.rstrip('.0')

    sheet = document.worksheet("prices")
    data = sheet.get_all_values()
    headers = data.pop(0)

    base = pd.DataFrame(data, columns=headers)
    base.rename(columns={"AXAPTA": "Articulo", "Costo Unitario 2024":"Precio Unitario"}, inplace=True)

    base["Cliente_UPC"] = base["CLIENTE"] + base["UPC"].astype(str)
    df["Cliente_UPC"] = df["Cliente"].astype(str) + df["Ean/Upc"].astype(str)

    df_result = pd.merge(df, base[['Cliente_UPC', "Precio Unitario","IEPS", "Articulo"]], on='Cliente_UPC', how='inner')
    df_result['IEPS'] = df_result['IEPS'].astype(int)
    df_result["Grupo de impuestos sobre las ventas"] = "IEPS" + (df_result['IEPS']).astype(str)
    df_result["Precio Unitario"] = round(df_result["Precio Unitario"], ndigits=2)

    df_result = df_result.loc[:, ['Orden_Compra', 'Cliente', 'Articulo', 'Cantidad', 'Unidad', 'Tamaño', 'Precio Unitario', 'Grupo de impuestos sobre las ventas', 'Grupo de impuestos por venta de articulos']]
    df_result.columns = df_result.columns.str.replace(' ', '_')

    orders_tag = ET.Element("Ordenes")
    orders_numbers = set(df["Orden_Compra"])


    for order_number in orders_numbers:
        order_tag = ET.SubElement(orders_tag, "Orden")

        list_ord = df_result.query(f"Orden_Compra == '{order_number}'")
        try:
          client = list_ord["Cliente"].iloc[0]
          sheet_accounts = document.worksheet("accounts")
          data = sheet_accounts.get_all_values()
          headers = data.pop(0)
          df_accounts = pd.DataFrame(data, columns=headers)
          id_account = str(df_accounts.query(f"Cliente == '{client}'")["Cuenta"].iloc[0])
        except:
          id_account = None

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
