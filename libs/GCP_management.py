from google.cloud import storage
import pandas
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
from google.cloud.storage import Blob


file_key = "/Users/mohamedkabadiabakhate/Downloads/digital-africa-rainbow-9a81b3086005.json"


class Cloud_storage_management(object):
    def __init__(self, file_key= file_key):
        """Iniatilise le client avec service account"""
        self.storage_client = storage.Client.from_service_account_json(file_key)
        self.buckets = list(self.storage_client.list_buckets())


    def list_blobs(self,bucket_name):
        """Lists all the blobs in the bucket."""
        blobs = self.storage_client.list_blobs(bucket_name)
        list_files = [element.name for element in blobs]
        map_files = {element.split('/')[0]:[e for e in list_files if element.split('/')[0] in e] for element in list_files}
        return bucket_name, map_files, list_files

    def blob_metadata(self,bucket_name, blob_name):
        """Prints out a blob's metadata."""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        return {
        'Blob' : blob.name,
        'Bucket' : blob.bucket.name,
        'Storage class' : blob.storage_class,
        'ID' : blob.id,
        'Size' : blob.size,
        'Updated' : blob.updated,
        'Generation' : blob.generation,
        'Metageneration' : blob.metageneration,
        'Etag' : blob.etag,
        'Owner' : blob.owner,
        'Component count' : blob.component_count,
        'Crc32c' : blob.crc32c,
        'md5_hash' : blob.md5_hash,
        'Cache-control' : blob.cache_control,
        'Content-type ': blob.content_type,
        'Content-disposition' : blob.content_disposition,
        'Content-encoding' : blob.content_encoding,
        'Content-language' : blob.content_language,
        'Metadata' : blob.metadata,
        'Custom Time' : blob.custom_time,
        'Temporary hold' : "enabled" if blob.temporary_hold else "disabled",
        'Event based hold' : "enabled" if blob.event_based_hold else "disabled",
        'retentionExpirationTime': blob.retention_expiration_time if blob.retention_expiration_time else ''
        }


    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )
    def get_meta(self, bucket_name):
        return pandas.DataFrame([self.blob_metadata(bucket_name,e) for e in list_files])

    def put_meta(self, bucket_name):
        self.get_meta(bucket_name).to_csv('meta_gcs.csv')
        self.upload_blob(bucket_name,'meta_gcs.csv', 'gcs_meta.csv' )
        
    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"


        bucket = self.storage_client.bucket(bucket_name)

        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
            "Blob {} downloaded to {}.".format(
                source_blob_name, destination_file_name
            )
        )




class Bigquery_management(object):
    def __init__(self, file_key= file_key):
        """Iniatilise le client avec service account"""
        self.client = bigquery.Client.from_service_account_json(file_key)
        self.project = self.client.project
        
    def list_dataset(self):
        """Construct a BigQuery client object."""

        datasets = list(self.client.list_datasets())  # Make an API request.
        project = self.client.project
        output = []

        if datasets:
            for dataset in datasets:
                output.append((dataset.dataset_id, self.client.get_dataset(dataset.dataset_id).description))
        else:
            print("{} project does not contain any datasets.".format(project))

        return output
    
    def list_all_table(self):
        """Construct a BigQuery client object"""

        client = self.client
        output = []
            
        for dataset_i in list(self.client.list_datasets()):
            print(dataset_i.dataset_id)
            tables = self.client.list_tables(dataset_i.dataset_id)  # Make an API request.

            for table in tables:
                project_id,dataset_id, table_id = (table.project, table.dataset_id, table.table_id)
                dataset_description = client.get_dataset(dataset_i.dataset_id).description
                dataset_location = client.get_dataset(dataset_i.dataset_id).location
                table_obj = client.get_table('{project_id}.{dataset_id}.{table_id}'.format(project_id= project_id, dataset_id= dataset_id, table_id = table_id))
                line = (project_id, dataset_id,dataset_description, dataset_location, table_id, table_obj.description, table_obj.num_rows)
                output.append(line)
        return pandas.DataFrame(output, columns = ['project_id','Dataset_id', 'Dataset_description', 'Dataset_location', 'Table_name','table_description', 'rows'])

    def upload_dataframe(self, df, project_id, dataset_id, table_id, if_exists = 'fail'):
        """Upload un dataframe vers Bigquery
        if_exists :['fail', 'replace','append']"""
        credentials = service_account.Credentials.from_service_account_file(file_key)
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = project_id
        table_id = '{dataset_id}.{table_id}'.format(dataset_id=dataset_id,table_id=table_id)
        df.to_gbq(table_id, if_exists=if_exists)
    
    def load_df(self, query):
        df = pandas_gbq.read_gbq(query, project_id = self.project)
        return df
    
    def export_gcs(self, dataset_id, table_id, bucket_name, blob):
        client = self.client

        destination_uri = "gs://{}/{}".format(bucket_name, blob)
        dataset_ref = bigquery.DatasetReference(self.project, dataset_id)
        table_ref = dataset_ref.table(table_id)

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
        )  # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
        )
        
    def export_to_csv(self,dataset_id, table_id, bucket_id, blob_id, local_path):
        self.export_gcs(dataset_id, table_id, bucket_id, blob_id)
        Cloud_storage_management().download_blob(bucket_id, blob_id, local_path)
        
    def upload_from_cloud_storage(self,dataset_id,table_id,bucket_id, blob_id, schema_tuple):
        # Construct a BigQuery client object.
        client = self.client

        # TODO(developer): Set table_id to the ID of the table to create.
        # table_id = "your-project.your_dataset.your_table_name"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(element[0], element[1]) for element in schema_tuple],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = "gs://{}/{}".format(bucket_id, blob_id)
        table_ref = '{}.{}.{}'.format(self.project,dataset_id,table_id)


        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_ref)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))      