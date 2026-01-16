from xml.dom import minidom
from zoneinfo import ZoneInfo
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
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
import smtplib
from datetime import date
import ssl


app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
app.config["JWT_ACCESS_COOKIE_NAME"] = "access_token_cookie"
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=10)
app.config["JWT_COOKIE_CSRF_PROTECT"] = False

swagger = Swagger(app)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["120 per day", "60 per hour"],
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

SENDER_EMAIL = os.getenv('SENDER_EMAIL')
PASSWORD_EMAIL = os.getenv('PASSWORD_EMAIL')
RECEIVER_EMAIL = os.getenv('RECEIVER_EMAIL')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 465))

credent = service_account.Credentials.from_service_account_file(filename="credentials.json", scopes=SCOPES)
client = gspread.authorize(credent)
document = client.open_by_key(SPREADSHEET_ID)

jwt = JWTManager(app)

def get_df_sheet(sheet_name):
    sheet = document.worksheet(sheet_name)
    data = sheet.get_all_values()
    headers = data.pop(0)   
    df = pd.DataFrame(data, columns=headers)
    return df

def check_column_df(df, column_name):
  return True if column_name in df.columns else False

def get_purcharses_server():   
  try:
    ssh_conn = paramiko.Transport((hostname, 22))
    ssh_conn.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(ssh_conn)

    files = sftp.listdir_attr(path)
    list_df = []

    for file in files:
      date_file = datetime.fromtimestamp(file.st_mtime, tz=ZoneInfo("America/Mexico_City")).strftime('%d-%m-%Y')
      if file.filename.lower().endswith("ord_compra.csv"):
        dic = {"filename": file.filename, "date": date_file}
        with sftp.open(path + dic["filename"], "r") as ftp_file:
          ftp_file.prefetch()
          df = pd.read_csv(ftp_file, sep=",", encoding_errors="ignore")
          df["Orden Compra"] = df["Orden Compra"].astype(str).str.rstrip('.0')
          df["Ean/Upc"] = df["Ean/Upc"].astype(str) #.str.rstrip('.0')
          df["Fecha"] = dic["date"]
          df["Prefijo"] = file.filename[:2] #El prefijo nos sirve para identificar al cliente, ya que algunos csv no se menciona
          list_df.append(df)
    sftp.close()
    ssh_conn.close()

    full_df = pd.concat(list_df, ignore_index=True)

    full_df["Cantidad"] = full_df["Cantidad"].astype(float)
    #Esto es para calcular el total de unidades, ya que las columnas varian entre cadenas
    if check_column_df(full_df, "Paq X Empaque"):
    #Esto es para calcular el total de unidades, ya que las columnas varian entre cadenas
      full_df.loc[full_df['Prefijo'] == "03", 'Cantidad'] = full_df["Paq X Empaque"] * full_df["Cantidad"]
    if check_column_df(full_df, "Piezas X Emp"):
      full_df.loc[full_df['Prefijo'].isin(["22"]), 'Cantidad'] = full_df["Cantidad"] * full_df["Piezas X Emp"]
    full_df.loc[full_df['Prefijo'].isin(["04", "05", "09", "40"]), 'Cantidad'] = full_df['Cantidad']

    #Para calcular los costo, se toma el costo de los archivos
    if check_column_df(full_df, "Costo"):
      full_df.loc[full_df['Prefijo'].isin(["03", "04", "05"]), 'Precio_Unitario_Ftp'] = full_df['Costo']
    if check_column_df(full_df, "Costo Uni") and check_column_df(full_df, "Empaque"):
      full_df.loc[full_df['Prefijo'].isin(["09", "22"]), 'Precio_Unitario_Ftp'] = full_df["Costo Uni"] / full_df["Empaque"]

    full_df.rename(columns={"Cadena": "Cliente", "Orden Compra": "Orden_Compra", "Cuenta_Facturacion": "Cliente"}, inplace=True)
    full_df = full_df[["Orden_Compra","Ean/Upc", "Precio_Unitario_Ftp", "Cliente","Cantidad", "Fecha", "Prefijo"]]

    df_items = get_df_sheet("Articulos")
    df_clients = get_df_sheet("Clientes")

    full_df["Prefijo_UPC"] = full_df["Prefijo"] + "_" + full_df["Ean/Upc"]
    full_df = pd.merge(full_df[["Orden_Compra", "Cliente", "Precio_Unitario_Ftp","Ean/Upc", "Cantidad", "Fecha", "Prefijo", "Prefijo_UPC"]], df_items[["UPC", "IEPS", "Tamaño", "Codigo_Axapta", "Precio_Unitario", "Prefijo_UPC", "Estilo", "Botellas_Caja"]], on="Prefijo_UPC", how='left') 
    full_df["Botellas_Caja"] = full_df["Botellas_Caja"].astype(float)
    full_df.loc[full_df['Prefijo'].isin(["40"]), 'Cantidad'] = full_df['Cantidad'] * full_df["Botellas_Caja"]
    full_df = full_df[["Orden_Compra", "Cliente", "Ean/Upc", "Precio_Unitario_Ftp", "Cantidad", "Fecha", "Prefijo", "Tamaño", "Codigo_Axapta", "IEPS", "Precio_Unitario", "Estilo", "Ean/Upc"]]
    full_df.loc[(full_df["Precio_Unitario_Ftp"] !=  full_df["Precio_Unitario"]) & (full_df["Precio_Unitario_Ftp"].notna()), 'Precio_Unitario'] = full_df["Precio_Unitario_Ftp"] 

    full_df = pd.merge(full_df, df_clients[["Cuenta_Facturacion", "Pre_Masteredi"]], left_on='Prefijo', right_on='Pre_Masteredi', how='left') 
    full_df["Precio_Unitario_Ftp"] = round(full_df["Precio_Unitario"], 2)
    full_df.rename(columns={"Cuenta_Facturacion": "Cliente"}) 
    full_df = full_df[["Orden_Compra", "Cliente", "Codigo_Axapta","Cantidad", "Precio_Unitario", "Tamaño", "Fecha", "IEPS", "Estilo", "Prefijo"]]

    sheet_orders = get_df_sheet("Processed_Orders")
    processed_orders = set(sheet_orders["ORDEN_COMPRA"])
    full_df =  full_df[~full_df["Orden_Compra"].isin(processed_orders)]
    return full_df
  except Exception as e:
    return e


def last_filled_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return len(str_list)


def get_df_orders_details(df_orders): 
    #df_orders = get_df_sheet("Processed_Orders")
    df = pd.DataFrame(columns=['Articulo_Axapta', 'Descripcion', 'UPC', 'Cantidad', 'Precio_Unitario'])
    import ast
    sheet_sales_manager = get_df_sheet("Responsables_Ventas")

    df_orders = df_orders.merge(sheet_sales_manager, on="NOMBRE_COMERCIAL_CLIENTE", how="left")

    orders = list(df_orders["ORDEN_COMPRA"])
    for order_id in orders: 
        df_row = df_orders[df_orders["ORDEN_COMPRA"] == order_id]
        df_row = df_row["DETALLES_ORDEN"].values[0]
        order = df_row.replace('nan', 'None')
        list_of_dicts = ast.literal_eval(order)
        df_temp = pd.DataFrame(list_of_dicts)
        df_temp["ORDEN_COMPRA"] =order_id
        df = pd.concat([df, df_temp], ignore_index=True)
        
        df = df[['ORDEN_COMPRA', 'Articulo_Axapta', 'Descripcion', 'UPC', 'Cantidad', 'Precio_Unitario']]

    df_details = df.merge(df_orders, how="left", on="ORDEN_COMPRA")
    df_details = df_details[['ORDEN_COMPRA', 'ORDEN_ID_AXAPTA', 'NOMBRE_COMERCIAL_CLIENTE',
        'FECHA', 'HORA', 'Articulo_Axapta', 'Descripcion', 'UPC', 'Cantidad',
        'Precio_Unitario', "Responsable de Ventas"]]

    df_details.columns = df_details.columns.str.upper()
    df_details = df_details.fillna("N/A")
    return df_details

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
@jwt_required()
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

@app.route('/get_all_purcharses', methods=['GET'])
@jwt_required()
def get_purcharses_day():
    """
    Endpoint to get all the purchases of a specific date in json format.
    Endpoint para obtener las ordenes de compra del servidor de Masteredi. Son todas las ordenes sin importar si ya han sido procesadas o no.
    ---
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
    return Response(df.to_json(orient="records"), mimetype='application/json')

@app.route('/post_purcharses', methods=['POST'])
@jwt_required()
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
              - Orden_ID_Axapta
            properties:
              Orden_Compra:
                type: string
                default: "UNIQUE_VALUE"
              Orden_ID_Axapta:
                type: string
                default: "1"
    responses:
      200:
        description: The product inserted in the database
    """

    try:
      content = request.get_json()
      df = get_purcharses_server()
      df_items = get_df_sheet("Articulos")
      df_clients = get_df_sheet("Clientes")
      orders=[]
      date_process = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%d-%m-%Y")
      time_process = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%H:%M")
      sheet_orders = document.worksheet("Processed_Orders")
      sheet_orders_id = set(get_df_sheet("Processed_Orders")["ORDEN_COMPRA"])
      for sale_order in content:
        id_order = sale_order["Orden_Compra"]
        if id_order not in sheet_orders_id:
          df_filtered = df[(df['Orden_Compra'] == id_order)]
          prefix =  df_filtered["Prefijo"].values[0]
          if not df_filtered.empty:
            items_orden = set(df_filtered["Codigo_Axapta"])
            order_details =[]
            for axapta_item in items_orden:
                row = df_items[(df_items["Prefijo"] == prefix) & ((df_items["Codigo_Axapta"] == axapta_item))]
                if row.empty == False:
                    description = row["Descripcion"].values[0]
                    upc = row["UPC"].values[0]
                    quantity = df_filtered[df_filtered["Codigo_Axapta"] == axapta_item]["Cantidad"].values[0]
                    unit_price = row["Precio_Unitario"].values[0]
                else:
                    description, upc, quantity, unit_price  = None, None, None, None
                elem = {"Articulo_Axapta": axapta_item, "Descripcion": description, "UPC": upc, "Cantidad": str(quantity), "Precio_Unitario": unit_price}
                order_details.append(elem)
            orders.append({"Orden_Compra": sale_order["Orden_Compra"], "Prefijo": prefix, 'Orden_ID_Axapta': sale_order['Orden_ID_Axapta'], "Fecha": date_process, "Hora": time_process, "Detalles_Orden": str(order_details)})
          else:
              pass
        else:
          print(f"Order: {id_order} is already in the processed orders list")

      df_orders = pd.DataFrame(orders)
      if not df_orders.empty:
        df_orders = df_orders.merge(df_clients, how="left", left_on="Prefijo", right_on="Pre_Masteredi")
        df_orders = df_orders[['Orden_Compra', 'Orden_ID_Axapta', 'Nombre_Comercial_Cliente', 'Fecha', 'Hora',
            'Detalles_Orden']]
        df_orders.columns = [e.upper() for e in df_orders]
        df_orders["FECHA"] = pd.to_datetime(df_orders["FECHA"], format="%d-%m-%Y")
        df_orders["FECHA"] = df_orders["FECHA"].dt.strftime('%Y-%m-%d')
        sheet_orders.append_rows(df_orders.values.tolist())

        df_details_orders = get_df_orders_details(df_orders)

        sheet_orders_details_sheet = document.worksheet("Processed_Details")
        sheet_orders_details_sheet.append_rows(df_details_orders.values.tolist())
        
        details_df_email = get_df_sheet("Processed_Details")
        today = date.today()

        """buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            details_df_email.to_excel(writer, sheet_name='Datos', index=False)
        buffer.seek(0)  # Volver al inicio del buffer"""

        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['Subject'] = f"Ordenes procesadas Masteredi - Axapta {today}"

        lista_destinatarios = [email.strip() for email in RECEIVER_EMAIL.split(',')]
        msg['To'] = ', '.join(lista_destinatarios)  # Para el encabezado del correo
            
        msg.attach(MIMEText(f'Buenas tardes, se adjunta sheet con ordenes procesadas a {today}', 'plain'))
        """excel_attachment = MIMEApplication(buffer.read())
        excel_attachment.add_header('Content-Disposition', 'attachment', filename=f'reporte_masteredi_{today}.xlsx')
        msg.attach(excel_attachment)"""

        context_ssl = ssl.create_default_context()
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context_ssl) as smtp:
                smtp.login(SENDER_EMAIL, PASSWORD_EMAIL)
                smtp.sendmail(SENDER_EMAIL, lista_destinatarios, msg.as_string())
                print("email sent")
        except Exception as e:
            print(f"Email not send: {e}")
        return "The orders were updated in the database"
      else:
          return "The order is already in the database"
    except Exception as e:
      return "Error " + str(e)
    

# Endpoint para retornar el XML
@app.route('/get_xml_purcharses/<date>')
@jwt_required()
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

    df = df.drop(['Fecha'], axis=1)

    sheet_sales_manager = get_df_sheet("Responsables_Ventas")
    sheet_sales_manager = sheet_sales_manager[["Cuenta Facturacion", "Responsable de Ventas", "Pago", "Zona de Ventas", "Forma de Pago"]]
    sheet_sales_manager.columns = sheet_sales_manager.columns.str.replace(' ', '_')
    
    orders_tag = ET.Element("Ordenes")
    orders_numbers = set(df["Orden_Compra"])

    for order_number in orders_numbers:
        order_tag = ET.SubElement(orders_tag, "Orden")

        list_ord = df.query(f"Orden_Compra == '{order_number}'")
        try:
            prefix_client = list_ord["Prefijo"].iloc[0]
            sheet_accounts = document.worksheet("Clientes")
            data = sheet_accounts.get_all_values()
            headers = data.pop(0)
            df_accounts = pd.DataFrame(data, columns=headers)
            id_account = str(df_accounts.query(f"Pre_Masteredi == '{prefix_client}'")["Cuenta_Facturacion"].iloc[0])
        except:
            id_account = "Not found"

        df_header = pd.DataFrame({
        'Orden_Compra': [order_number],
        "Cliente": [id_account],
        'Sitio': ["Vitivinico"], 
        'Almacen': ["IZTAPALAPA"], 
        'Departamento': ["deo"], 
        'Centro_de_costo': ["VEVEVEVL1"],
        'Reporte': [""], 
        'Tipo_de_Gasto': ["OPROP"], 
        'Financiera': ["TRANOF"], 
        'Proposito': ["DIS"], 
        'Tesoreria': ["00004000"],
        'Empresa_Id': ["deo"]
        })

        df_header = df_header.merge(sheet_sales_manager, how="left", left_on="Cliente", right_on="Cuenta_Facturacion")
        df_header = df_header[[
        'Orden_Compra',
        "Cliente",
        'Sitio',
        'Almacen',
        'Departamento',
        'Centro_de_costo',
        'Reporte',
        'Tipo_de_Gasto',
        'Financiera',
        'Proposito',
        'Tesoreria',
        'Empresa_Id', 
        "Cuenta_Facturacion", 
        "Responsable_de_Ventas", 
        "Pago", "Zona_de_Ventas", 
        "Forma_de_Pago"]]

        df_header = df_header.drop("Cuenta_Facturacion", axis=1)
        df_header["Tipo_Gasto"] = ""

        root_df_header = ET.fromstring(df_header.to_xml(index=False))
        for row in root_df_header:
            header_tag = ET.SubElement(order_tag, "Cabecera")
            for child in row:
                child_element = ET.SubElement(header_tag, child.tag)
                child_element.text = child.text

        concepts_tag= ET.SubElement(header_tag, "Conceptos")
        list_ord = list_ord.drop(['Orden_Compra','Cliente', 'Prefijo'], axis=1)

        list_ord = list_ord.rename(columns = {"IEPS": "Grupo_de_impuestos_sobre_las_ventas"})
        list_ord["Unidad"] = "PIEZA"
        list_ord["Grupo_de_impuestos_por_venta_de_articulos"] = "BEB"

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


@app.route('/delete_processed_purcharses', methods=["delete"])
@jwt_required()
def delete_processed_purcharses():
  """
  Endpoint to get all the processed purchases.
  ---
  responses:
    200:
      description: All the purcharses
  """
  sheet_orders = document.worksheet("Processed_Orders")
  sheet_orders.delete_rows(start_index=2, end_index=last_filled_row(sheet_orders))
  return "Sucess"


@app.route('/get_all_processed_purcharses')
@jwt_required()
def get_all_processed_purcharses():
  """
  Endpoint to get all the specific date unprocessed purchases in json format.
  ---
  responses:
    200:
      description: All the purcharses
  """
  sheet_orders = get_df_sheet("Processed_Orders")
  if sheet_orders.empty:
    return "There is no processed orders"
  else:
    return Response(sheet_orders.to_json(orient="records"), mimetype='application/json')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)






