import os
from os import listdir
from os.path import isfile, join
import time
from google.cloud import storage
from concurrent.futures import TimeoutError
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
    def __init__(self, bucket_name, project_id, topic_id, subscription_id):
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.topic_id = topic_id
        self.subscription_id = subscription_id
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(self.bucket_name)
    
        self.publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        self.subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_id}`
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)


    def upload_files_to_bucket(self, path: str, files: list):

        for filename in files:
            fullpath = os.path.join(path, filename)
            blob = self.bucket.blob(filename)
            blob.upload_from_filename(fullpath)
            print(f"Uploaded: {blob.public_url}")

    def download_from_bucket(self, filename: str, path: str):
            blob = self.bucket.blob(filename)
            
            # Create folder if it doesn't exist yet
            if not os.path.exists(path):
                os.makedirs(path)
            blob.download_to_filename(os.path.join(path, filename))

    def download_from_bucket(self, filename: str):
            blob = self.bucket.blob(filename)
            blob.download_to_filename(filename)

    
    def publish_message(self):

        for n in range(1, 10):
            data_str = f"Message number {n}"
            # Data must be a bytestring
            data = data_str.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = self.publisher.publish(self.topic_path, data)
            print(future.result())

        print(f"Published messages to {self.topic_path}.")

    def pull_messages(self, timeout):

        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}..\n")

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                result = streaming_pull_future.result(timeout=timeout)
                print(result)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")

        filename = message.data.decode('utf-8')
        print(f"Uploaded filename : {filename}")
        
        # Download file from bucket
        path = "downloaded_files"
        self.download_from_bucket(filename)
        
        # Load file and square
        with open(filename) as f:
            data = f.read()
            number = float(data)
            print(f"File contains: {number}")

            number_squared = square_number(number)
            print(f"The number squared is: {number_squared}")

        message.ack()

def square_number(number: float) -> float:
    return number * number


def main():
    print("Hello world!")

    gc = GoogleCloudHelper(
        bucket_name=os.getenv('BUCKET_NAME'), 
        project_id=os.getenv('PROJECT_ID'),
        topic_id=os.getenv('TOPIC_ID'),
        subscription_id=os.getenv('SUBSCRIPTION_ID')
    )

    gc.pull_messages(timeout=1000)


    # watcher = FileWatcher(
    #                 path='/data',
    #                 interval=1,
    #                 gc_helper=gc,
    # )

    # watcher.watch()


def get_files_in_folder(path: str):
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles

def get_list_differences(list1: list, list2: list):
    difference = set(list1) ^ set(list2)
    return list(difference)




if __name__=="__main__":
    main()