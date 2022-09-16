import os
from os import listdir
from os.path import isfile, join
import time
from google.cloud import storage
from google.cloud import pubsub_v1

class FileWatcher:
    """Periodically checks a specific path for new files, returns the new files when they are found"""
    def __init__(self, path: str, interval: str, gc_helper):
        self.path = path
        self.interval = interval
        self.current_files = get_files_in_folder(self.path)
        self.gc_helper = gc_helper

    def watch(self):
        while True:
            files = get_files_in_folder(self.path)
            difference = get_list_differences(files, self.current_files)

            if len(difference) > 0:
                self.gc_helper.upload_files_to_bucket(self.path, difference)
                print(f"Found new files: {difference}")
            else:
                print("Found no differences")
            
            self.current_files = files
            time.sleep(self.interval)






class GoogleCloudHelper():
    def __init__(self, bucket_name, project_id, topic_id):
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.topic_id = topic_id
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(self.bucket_name)
    
        self.publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def upload_files_to_bucket(self, path: str, files: list):

        for filename in files:
            fullpath = os.path.join(path, filename)
            blob = self.bucket.blob(filename)
            blob.upload_from_filename(fullpath)
            self.publish_message(filename)
            print(f"Uploaded: {blob.public_url}")
            
    
    def publish_message(self, message: str):

            data = message.encode("utf-8")

            # When you publish a message, the client returns a future.
            future = self.publisher.publish(self.topic_path, data)
            print(future.result())

            print(f"Published message to {self.topic_path}.")




def main():
    print("Hello world!")

    gc = GoogleCloudHelper(
        bucket_name=os.getenv('BUCKET_NAME'), 
        project_id=os.getenv('PROJECT_ID'),
        topic_id=os.getenv('TOPIC_ID')
    )

    # gc.publish_message()
    watcher = FileWatcher(
                    path='/data',
                    interval=1,
                    gc_helper=gc,
    )

    watcher.watch()


def get_files_in_folder(path: str):
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles

def get_list_differences(list1: list, list2: list):
    difference = set(list1) ^ set(list2)
    return list(difference)




if __name__=="__main__":
    main()