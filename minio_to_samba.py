#!/usr/bin/python3
import smbclient
from minio import Minio
from minio.error import S3Error
import logging
import os
import json

def write_smb_file(data: dict):
    """
    data = {"filename": "", "file_data": ""}
    """
    smb_string = data['samba_data']['path']
    username = data['samba_data']['username']
    password = data['samba_data']['password']
    path = "\\".join([smb_string, data['filename']])

    smbclient.ClientConfig(username=username, password=password)

    with smbclient.open_file(path, mode="w") as fd:
        file_content = fd.write(data['file_data'])


def main():
    
    with open("settings.json", "r") as fp:
        settings = json.load(fp)

    client = Minio(
        f"{settings['minio']['ip']}:{settings['minio']['port']}",
        access_key=f"{settings['minio']['access_key']}",
        secret_key=f"{settings['minio']['secret_key']}",
        secure=False
    )

    try:
        files_folder = "files_history/"
        # Listamos los archivos en la carpeta local "edifacts"
        files_in_folder = os.listdir(files_folder)

        # Creamos el objeto que representa la carpeta en MinIO
        objects = client.list_objects(settings['minio']['folder'])
        files_in_minio = []

        print("Files in local folder:", len(files_in_folder))
        for item in objects:
            if str(item.object_name).startswith(settings['minio']['prefix']):
                files_in_minio.append(item.object_name)
        print("Files in MinIO:", len(files_in_minio))

        # Comparamos las listas "files_in_minio" y "files_in_folder" en busca de diferencias.
        # Si existen diferencias, creamos una nueva lista con los archivos que estan en MinIO
        # pero no estan en la carpeta local. Si no hay diferencias, se crea una lista vacia.
        s = set(files_in_folder)
        new_list = [x for x in files_in_minio if x not in s]

        print(f"[+] Se descargaran {len(new_list)} archivos a la carpeta 'edifacts/'...")
        objects = client.list_objects(settings['minio']['folder'])
        for item in objects:
            # Descargamos el archivo en la carpeta "files_history"
            if str(item.object_name).startswith(settings['minio']['prefix']) and str(item.object_name) not in files_in_folder:
                print("- File:", item.object_name)
                client.fget_object(
                    bucket_name=settings['minio']['folder'],
                    object_name=item.object_name,
                    file_path=files_folder + item.object_name
                )
        # Abrimos el archivo "files_history.json" para saber que archivos ya fueron enviados a SMB
        # asi evitamos volver a enviarlos.
        with open("files_history.json", "r") as jsonfile:
            files_history = json.load(jsonfile)
            print(f"[+] Existen {len(files_history['files_history'])} archivos en el historico de INPNRPROD")

        files_in_folder = os.listdir(files_folder)
        # Buscamos diferencias entre los archivos edifact existentes entre la
        # lista "files_in_folder" y el historico de archivos edifact enviado "files_history"
        files_to_send_to_smb = [x for x in files_in_folder if x not in files_history['files_history']]

        print(f"[+] Se enviaran {len(files_to_send_to_smb)} archivos a los samba server...")
        files_tmp_history = []

        for edi in files_to_send_to_smb:
            for samba in settings['samba_servers']:
                print(f"- Enviando archivo {edi} a {samba['path']}...")
                
                # Abrimos el archivo, leemos el contenido y se lo enviamos a la funcion
                # "write_smb_file" para enviar el archivo a INPNRPROD
                with open(files_folder+edi, "r") as fp:
                    file_data = fp.read()
                    write_smb_file({"filename": edi, "file_data": file_data, "samba_data": samba})
            
            files_tmp_history.append(edi)

        # Guardamos en el historial "files_history.json" los nuevos archivos ya enviados...
        file_list = files_history['files_history']
        file_list.extend(files_tmp_history)
        files_history['files_history'] = file_list
        json_object = json.dumps(files_history, indent=4)

        with open("files_history.json", "w") as jsonfile:
            jsonfile.write(json_object)
            print("[+] Archivo files_history.json actualizado correctamente.")

    except:
        logging.critical("Object storage not reachable")


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
