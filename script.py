#!/usr/bin/python
import os, sys, datetime, tarfile, os.path
from pymongo import MongoClient
from bson.json_util import dumps
import shutil
from azure.storage.blob import  BlobServiceClient, BlobClient, ContainerClient

def create_folder_backup(db_name):
    directory = ('backups/%s-%s-%s/%s' % (dt.month,dt.day,dt.year, db_name))
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def run_backup(mongoUri, dbname):
    client = MongoClient(mongoUri)
    db = client[dbname]
    collections = db.list_collection_names()
    directory = create_folder_backup(dbname)
    for collection in collections:
        db_collection = db[collection]
        cursor = db_collection.find({})
        filename = ('%s/%s.json' %(directory,collection))
        with open(filename, 'w') as file:
            file.write('[')
            for document in cursor:
                file.write(dumps(document))
                file.write(',')
            file.write(']')

def tar_and_upload():
    date = '%s-%s-%s' % (dt.month, dt.day, dt.year)
    tar = tarfile.open('backups/%s.tar.gz' % (date), "w:gz")
    tar.add('backups/%s/' % (date), arcname=date)
    tar.close()
    shutil.rmtree('backups/%s/' % (date))
    upload_to_azure_storage('%s.tar.gz' % (date))

def upload_to_azure_storage(file_name):
    print("\nUploading to Azure Storage as blob:\n\t" + file_name)
    connect_str = 'your_azure_storage_connection-string'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = 'yout_azure_container_name'
    container_client = blob_service_client.get_container_client(container_name)
    file_path = 'backups/%s' % (file_name)
    dest_path = ('daily_bak/%s' % (file_name))
    blob = container_client.get_blob_client(dest_path)
    with open(file_path, 'rb') as data:
        blob.upload_blob(data)

if __name__ == '__main__':
        print('[*] Script started')
        dt = datetime.datetime.now()
        username = 'your_username'
        password = 'your_password'

        # it can be replica set connection string mongodb1.example.com:27317,mongodb2.example.com:27017 or
        host = 'localhost:27017' 

        for db_to_backup in ["db1", "db2"]:
            mongoUri = ('mongodb://%s:%s@%s/%s?authMechanism=DEFAULT' % (username, password, host, db_to_backup))
            try:
                run_backup(mongoUri, db_to_backup)
                print('[*] %s successfully backed up!' % (db_to_backup))
            except Exception as e:
                print('[-] An unexpected error has occurred')
                print('[-] '+ str(e) )
        
        tar_and_upload()