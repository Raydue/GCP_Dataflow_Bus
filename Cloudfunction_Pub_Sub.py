import json
import requests
from google.cloud import pubsub_v1

def pubsub_tdx_data(request):
    TDX_API_URL = "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeNearStop/City/Taipei/617?%24top=30&%24format=JSON"
    TDX_API_HEADERS = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJER2lKNFE5bFg4WldFajlNNEE2amFVNm9JOGJVQ3RYWGV6OFdZVzh3ZkhrIn0.eyJleHAiOjE3MTk5MDgyMjEsImlhdCI6MTcxOTgyMTgyMSwianRpIjoiMjU0ODRhOGUtZjIzNC00ZjY3LWFjODAtMDljZTg5ZDAxN2Y5IiwiaXNzIjoiaHR0cHM6Ly90ZHgudHJhbnNwb3J0ZGF0YS50dy9hdXRoL3JlYWxtcy9URFhDb25uZWN0Iiwic3ViIjoiNjZkYmQyYzgtM2YyMS00ODIxLWI3MWItNGI0MzIzYzhlYTMxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoicmF5ZHVlMzgtMDAzYTc5MjMtNzRjYS00ZWNiIiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJzdGF0aXN0aWMiLCJwcmVtaXVtIiwicGFya2luZ0ZlZSIsIm1hYXMiLCJhZHZhbmNlZCIsImdlb2luZm8iLCJ2YWxpZGF0b3IiLCJ0b3VyaXNtIiwiaGlzdG9yaWNhbCIsImJhc2ljIl19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJ1c2VyIjoiOGE4ZGI2YzAifQ.b4xAkGDOqugD25wErhDfxuUSIZbWINlqiEp0wbVyvALR3xgWAq2AG69WmiPp_JHrgmUEYMGZp4u7DLaU4sDqzxBBlL3LBQO3h5qcaUlsra6-x0Kfh-02xVrI3vQAhmcGzbbN6j_ZtEU2fGoMaKuOe7qlbUqT7RsBnHmuJ7lFcn1VpWid66W9PhZY0sed3szBhizM9dN7eW4Ok56vwbh7tHCtm8nN-KQQsGk67tDBP3T3lKZ-GpEOfsEPGyzsUiboRWbfFolHTaGk76HgHk_DhITX6LubjJqo0PNVyt3LGAUKzxr_ldjH9Uq9WAGG0mo_BtuMTURzfn6KYABckCXUgQ'
    }

    publisher = pubsub_v1.PublisherClient()
    topic_path = 'projects/my-baby-project-422109/topics/demotopic'

    def fetch_data():
        print("Fetching data from TDX API")
        response = requests.get(TDX_API_URL, headers=TDX_API_HEADERS)
        print(f"Response status code: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data: {response.text}")
            response.raise_for_status()

    def publish_messages(data):
        print("Publishing messages to Pub/Sub")
        for entry in data:
            message_data = json.dumps(entry).encode('utf-8')
            future = publisher.publish(topic_path, data=message_data)
            print(f'Published message ID: {future.result()}')

    
    try:
        print("Cloud Function triggered")
        data = fetch_data()
        print(f"Fetched data: {data}")
        publish_messages(data)
        print("Data published to Pub/Sub")
        return 'Data published to Pub/Sub', 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return f"Error occurred: {e}", 500
