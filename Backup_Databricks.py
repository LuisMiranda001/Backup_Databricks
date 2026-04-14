import os
import json
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

inicio = time.time()
logging.basicConfig(filename="Summary.log", filemode="a", level=logging.INFO)

workspace_dict = {
    "bieno-da18-p-902572-adb-01": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_01"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_01")
    },
    "bieno-da18-p-902572-adb-03": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_03"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_03")
    },
    "bieno-da18-p-902572-adb-04": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_04"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_04")
    },
    "bieno-da18-p-902572-adb-05": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_05"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_05")
    },
    "bieno-da18-p-902572-adb-06": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_06"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_06")
    },
    "bieno-da18-p-902572-adb-07": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_07"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_07")
    },
    "bieno-da18-p-902572-adb-08": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_08"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_08")
    },
    "bieno-da18-p-902572-adb-09": {
        "Environment": "PROD",
        "URL": os.getenv("DATABRICKS_URL_PROD_09"),
        "Api-key": os.getenv("DATABRICKS_TOKEN_PROD_09")
    }
}

for ws, data_ws in workspace_dict.items():
    print(ws,data_ws["Environment"])
    if data_ws["Environment"] == "PROD":
        Headers = {"Authorization":f"Bearer {data_ws['Api-key']}","Content-Type": "application/json"}

        r_email = requests.get(f"https://{data_ws['URL']}/api/2.0/preview/scim/v2/Users", headers=Headers)
        r_email = json.loads(r_email.text)
        print(r_email["totalResults"], " users")
        for i in range(r_email["totalResults"]):
            email = r_email["Resources"][i]['emails'][0]['value']
            logging.info(f"{data_ws['Environment']} - {email}")
            print("Trabajando con: ", email)
            data = json.dumps({"path":f"/Users/{email}"})
            response = requests.get(f"https://{data_ws['URL']}/api/2.0/workspace/list",data=data, headers=Headers)
            response = json.loads(response.text)
            os.makedirs(f"BackupUsers/{data_ws['Environment']}/{ws}/{email}", exist_ok = True)
            if len(response) != 0:
                for i in range(len(response["objects"])):
                    if response['objects'][i]["object_type"] == "NOTEBOOK":
                        path = "/Workspace" + response['objects'][i]["path"]
                        logging.info(path)
                        data = json.dumps({"path":path,"format":"AUTO","direct_download":"true"})
                        r_notebook = requests.get(f"https://{data_ws['URL']}/api/2.0/workspace/export",data=data, headers=Headers)
                        file_name = path.split('/')[-1].replace(':','').replace('"','').replace('?','').replace('>','').replace('.','')

                        try:
                            #* Creamos el txt
                            with open(f"BackupUsers/{data_ws['Environment']}/{ws}/{email}/{file_name}.txt", 'w') as archivo_txt:
                                json.dump(r_notebook.content.decode('utf-8'), archivo_txt)
                            
                            #* Leemos archivo txt
                            with open(f"BackupUsers/{data_ws['Environment']}/{ws}/{email}/{file_name}.txt", "r", encoding="utf-8") as archivo:
                                contenido = archivo.read()
                                contenido = contenido.replace('\\"','"')
                            
                            #* Creamos el archivo .py colocando los saltos de linea
                            with open(f"BackupUsers/{data_ws['Environment']}/{ws}/{email}/{file_name}.py", "w", encoding="utf-8") as f:
                                f.write(contenido.strip().replace("\\n","\n"))
                            
                            os.remove(f"BackupUsers/{data_ws['Environment']}/{ws}/{email}/{file_name}.txt")
                        except json.JSONDecodeError as e:
                            print(f"JSON Decode Error: {e}")
                    else:
                        print("No se pudo hacer backup de: ",response['objects'][i]["object_type"])
            else:
                logging.info("No hay notebooks para guardar")

fin = time.time()
tiempo_transcurrido = float((fin - inicio)/60)
print(f"Tiempo transcurrido: {tiempo_transcurrido} minutos")