"""
Azure Blob Storage is optimized for storing massive amounts of unstructured data. Unstructured data is data that doesn't adhere to a 
particular data model or definition, such as text or binary data. Blob storage offers three types of resources:

The storage account
A container in the storage account
A blob in the container
The following diagram shows the relationship between these resources:

Use the following Python classes to interact with these resources:

BlobServiceClient: The BlobServiceClient class allows you to manipulate Azure Storage resources and blob containers.
ContainerClient: The ContainerClient class allows you to manipulate Azure Storage containers and their blobs.
BlobClient: The BlobClient class allows you to manipulate Azure Storage blobs.
"""
from azure.identity import InteractiveBrowserCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
from typing import Union, Type

def get_azure_credential(type: str ='default') -> Type['Azure Identity Credential']:
  """We need to Authenticate to Azure and authorize access to blob data to perform operations
  
  for that we need to create a credential. Azure provides multiple options for authentication using
  various types are credentials , here we have deault and interactive(browser based userdid/password authentication) 
  credentials.
  """
  if type == 'default':
    credential = DefaultAzureCredential()
  elif type == 'interactive':
    credential = InteractiveBrowserCredential()
  
  return credential
  
def get_blob_Service_client(STORAGE_ACCOUNT_URL: str, credential) -> Type['BlobServiceClient object]:
    """Get the BlobServiceClient class that allows you to manipulate Azure Storage resources and blob containers.

    Attributes
    ----------
      STORAGE_ACCOUNT_URL : URL string that identifies the azure storage account.
      credential : Credential object which is used for authentication.
    Returns
    -------
      BlobServiceClient object
    """
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(STORAGE_ACCOUNT_URL, credential=credential)
    return blob_service_client
                                                                     
def get_container_Service_client(blob_service_client, BLOB_STORAGE_CONTAINER_NAME: str) -> Type['ContainerServiceClient object']:
    """Get the ContainerClient class that allows you to manipulate Azure Storage containers and their blobs.

    Attributes
    ----------
      blob_service_client :  BlobServiceClient object.
      BLOB_STORAGE_CONTAINER_NAME : Name of the blob storage container inside the storage account linked with BlobServiceClient object.

    Returns
    -------
      ContainerServiceClient object
    """
    # Create the ContainerServiceClient object
    container_client = blob_service_client.get_container_client(container=BLOB_STORAGE_CONTAINER_NAME)
    return container_client
                                                                          
def get_blob_client(container_client, blob_file_name: str) -> Type['BlobClient object']:
    """Get the BlobClient class that allows you to manipulate Azure Storage blobs..

    Attributes
    ----------
      container_client :  ContainerServiceClient object.
      blob_file_name : Name of the blob file in the consiner linked to ContainerServiceClient object 
                       (please note that the folder structure you create in the container
                       is part of the file name, you may see folders when accessing the container but they are not actual
                       directories, they are virtual folders for display purposes, actually the directory 
                       structure(foldernames, slashes) are part of the file name.)
                       
                       ex:- file names :- 
                          1) Parent/Child/File.csv
                          2) reference.txt etc.
                       

    Returns
    -------
      BlobClient object
    """
    # Create the BlobClient object
    blob_client = container_client.get_blob_client(blob_file_name)
    
    return blob_client
                                                                          
def upload_file_to_blob():
    """Upload a File to Blob storage container using V2 Python SDK."""
    def show_file_progress(uploaded_size: Union[int, float], total_size: Union[int, float]):
        """Print Progress while uploading large Files."""
        bar_total_length = 20
        percentage_uploaded = int((uploaded_size*100) / total_size)
        current_bar_length = int(percentage_uploaded * bar_total_length / 100)
        progress_bar = '|' + '#'*current_bar_length + '|' + str(percentage_uploaded) + '% Completed' 
        print('\r' + progress_bar, end='', flush=True)
                                                                          
    interactive_credential = get_azure_credential('interactive')   # InteractiveBrowserCredential()                                                                  
    blob_service_client = get_blob_Service_client('STORAGE_ACCOUNT_URL', interactive_credential) # BlobServiceClient(STORAGE_ACCOUNT_URL, credential=interactive_credential))
    container_client = get_container_Service_client(blob_service_client, 'BLOB_STORAGE_CONTAINER_NAME') #  blob_service_client.get_container_client(container='BLOB_STORAGE_CONTAINER_NAME')
    file_name_with_folder_structure_on_blob = '{}/{}/{}'.format('parent_folder', 'child_folder', 'file_name.csv')
    file_path_to_read = "./filename.csv"                                                                                                                    
    try:
        # create so-called folder
        blob_client = get_blob_client(container_client, blob_file_name=file_name_with_folder_structure_on_blob)  # container_client.get_blob_client(blob_file_name)
        # upload blob/file
        with open (file_path_to_read, 'rb') as data:
            blob_client.upload_blob(data, progress_hook=show_file_progress)
    except ResourceExistsError as Error: # from azure.core.exceptions import ResourceExistsError
           print(f"you are trying to upload an existing file in the blob")
           break
                                                                          
def list_blobs_in_the_container(container_client, print_list: bool=True) -> list:
    """Get all the blob files in the container.Print the list if required
    
    usage : list_blobs_in_the_container(container_client, print_list=True)
    """
    print("\nListing blobs...")
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    if print_list:
        for blob in blob_list:
            print("\t" + blob.name)
    
    return [blob.name for blob in blob_list]

def delete_blob_file(container_client, file_name: str):
    """Delete a blob file in a azure blob storage container."""
    print(f"Deleting blob File -> {file_name}")
    container_client.delete_blobs(file_name)
    
def delete_all_blobs_that_matching_string_in_their_name(container_client, matching_string: Union[list, str]):
    """Delete all files associated with a sub string.
    
    This Function should be used if you wanted to delete
      1) files with a common string in their name.
      2) If you have a virtual folders setup in the blob storage container and 
         wanted to delete all files under that folder/occurrences of that folder 
         at different places in the virtual folder structure.
    
    usage: #delete_job_files(container_client, 'inputs_folder)
    """
    if isinstance(matching_string, str):
        name_list = [matching_string]
    for name in name_list:
        print(f"Deleting files of job -> {name}")
        blob_list = list_blobs_in_the_container(container_client, print_list=False)
        list_to_delete = [i for i in blob_list if name in i]
        for file_to_delete in list_to_delete:
            delete_blob_file(container_client, file_to_delete)                                                             
