import os
import zipfile
import config

def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted file: {file_path}")
    else:
        print(f"File does not exist: {file_path}")

def create_zip_file(zip_file_path, files_to_zip):
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files_to_zip:
            zipf.write(file, os.path.basename(file))
            print(f"Added {file} to zip file: {zip_file_path}")

def replace_storage_account_key():
    file_path_template = 'src/src-montecarlo-app/montecarlo_app_template.py'
    file_path = 'src/src-montecarlo-app/montecarlo_app.py'
    
    
    with open(file_path_template, 'r') as file:
        file_data = file.read()

    file_data = file_data.replace('##STORAGE_ACCOUNT_NAME##', config.STORAGE_ACCOUNT_NAME)
    file_data = file_data.replace('##STORAGE_ACCOUNT_KEY##', config.STORAGE_ACCOUNT_KEY)

    with open(file_path, 'w') as file:
        file.write(file_data)


if __name__ == '__main__':

    replace_storage_account_key()

    zip_file_path = './src/src-montecarlo-app/montecarlo-app.zip'
    files_to_zip = [
        './src/src-montecarlo-app/montecarlo_app.py'
    ]
    delete_file(zip_file_path)
    create_zip_file(zip_file_path, files_to_zip)

    app_file_path = 'src/src-montecarlo-app/montecarlo_app.py'
    delete_file(app_file_path)