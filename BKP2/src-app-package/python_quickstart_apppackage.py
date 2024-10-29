import os
import zipfile

def delete_zip_file(zip_file_path):
    """
    Deletes the zip file if it exists.

    :param zip_file_path: The path to the zip file.
    """
    if os.path.exists(zip_file_path):
        os.remove(zip_file_path)
        print(f"Deleted zip file: {zip_file_path}")
    else:
        print(f"Zip file does not exist: {zip_file_path}")

def create_zip_file(zip_file_path, files_to_zip):
    """
    Creates a zip file containing the specified files.

    :param zip_file_path: The path to the zip file.
    :param files_to_zip: A list of file paths to include in the zip file.
    """
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files_to_zip:
            zipf.write(file, os.path.basename(file))
            print(f"Added {file} to zip file: {zip_file_path}")

    
if __name__ == '__main__':
    zip_file_path = './src/src-app-package/app.zip'
    files_to_zip = [
        './src/src-app-package/app.sh'
    ]
    delete_zip_file(zip_file_path)
    create_zip_file(zip_file_path, files_to_zip)