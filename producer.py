import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']  # Use localhost:29092 when running outside docker
TOPIC_NAME = 'user_activity'

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Error creating producer: {e}")
        return None

def generate_event(user_id=None, late=False):
    event_types = ['page_view', 'click', 'session_start', 'session_end']
    urls = ['/home', '/products', '/cart', '/checkout', '/about']
    
    if late:
        # Event time is 3 minutes in the past
        event_time = (datetime.utcnow() - timedelta(minutes=3)).isoformat() + 'Z'
    else:
        event_time = datetime.utcnow().isoformat() + 'Z'
        
    return {
        'event_time': event_time,
        'user_id': user_id or f'user_{random.randint(1, 100)}',
        'page_url': random.choice(urls),
        'event_type': random.choice(event_types)
    }

def main():
    producer = create_producer()
    if not producer:
        return

    print("Starting data producer. Press Ctrl+C to stop.")
    
    users = [f'user_{i}' for i in range(1, 11)]
    
    try:
        while True:
            # Randomly decide to send a late event (approx 5% chance)
            is_late = random.random() < 0.05
            
            event = generate_event(user_id=random.choice(users), late=is_late)
            producer.send(TOPIC_NAME, event)
            
            if is_late:
                print(f"Sent LATE event: {event}")
            else:
                print(f"Sent event: {event}")
                
            time.sleep(1)  # Send one event per second
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
