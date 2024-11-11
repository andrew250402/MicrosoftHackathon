import requests
from azure.eventhub import EventHubProducerClient, EventData
import time
import json

EXCHANGE_RATE_API_URL = "https://api.exchangeratesapi.io/v1/latest"
API_KEY = 'cc00d2bdce83bfe85010f26804f5f456'

EVENT_HUB_CONNECTION_STR = 'Endpoint=sb://microsofthackathon.servicebus.windows.net/;SharedAccessKeyName=SendData;SharedAccessKey=LQHPhByIDzWMES7Z+s5czsnhbYvPpi3t7+AEhJKHpRI='
EVENT_HUB_NAME = 'currency_conversion'

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

def fetch_exchange_rates():
    params = {
        "access_key": API_KEY
    }
    response = requests.get(EXCHANGE_RATE_API_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print("Fetched data:", data)
        return data
    else:
        print(f"Failed to fetch data: {response.status_code}")
        print("Error details:", response.json())
        return None

def send_to_event_hub(data):
    try:
        event_data_batch = producer.create_batch()
        
        message = {
            "base": data["base"],
            "timestamp": data["timestamp"],
            "rates": data["rates"]
        }
        
        event_data = json.dumps(message)
        event_data_batch.add(EventData(event_data))
        producer.send_batch(event_data_batch)
        
        print("Data sent to Event Hub:", message)
    except Exception as e:
        print(f"Error sending data to Event Hub: {e}")

def main():
    while True:
        data = fetch_exchange_rates()
        
        if data:
            send_to_event_hub(data)
        
        time.sleep(15)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Data streaming stopped by user.")
    finally:
        producer.close()
