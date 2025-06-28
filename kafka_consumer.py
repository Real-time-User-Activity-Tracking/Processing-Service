#!/usr/bin/env python3
"""
Simple Kafka Consumer for Testing
Consumes messages from the user-activity-events topic produced by the Go Ingestion-Service
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SimpleKafkaConsumer:
    def __init__(self, read_from_beginning=False):
        self.brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
        self.topic = os.getenv('KAFKA_TOPIC', 'user-activity-events')
        # Use timestamp to create unique consumer group each time
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.group_id = os.getenv('KAFKA_GROUP_ID', f'testing-consumer-group-{timestamp}')
        self.read_from_beginning = read_from_beginning
        self.consumer = None
        
    def connect(self):
        """Connect to Kafka and subscribe to the topic"""
        try:
            print(f"Connecting to Kafka brokers: {self.brokers}")
            print(f"Subscribing to topic: {self.topic}")
            print(f"Consumer group: {self.group_id}")
            
            # Set offset reset based on parameter
            offset_reset = 'earliest' if self.read_from_beginning else 'latest'
            print(f"Offset reset: {offset_reset}")
            print("-" * 60)
            
            # Try to create consumer with snappy support first
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.brokers,
                    group_id=self.group_id,
                    auto_offset_reset=offset_reset,  # Use parameter
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda m: m.decode('utf-8') if m else None
                )
            except Exception as e:
                if "snappy" in str(e).lower():
                    print("Snappy compression detected but not supported. Installing snappy support...")
                    print("Please install snappy support: pip install python-snappy")
                    print("Trying alternative approach...")
                    
                    # Try without compression handling
                    self.consumer = KafkaConsumer(
                        self.topic,
                        bootstrap_servers=self.brokers,
                        group_id=self.group_id,
                        auto_offset_reset=offset_reset,  # Use parameter
                        enable_auto_commit=True,
                        # Use raw bytes deserializer to handle compression
                        value_deserializer=lambda m: self.try_decode_message(m),
                        key_deserializer=lambda m: m.decode('utf-8') if m else None
                    )
                else:
                    raise e
            
            print("Successfully connected to Kafka!")
            return True
            
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            print("Make sure:")
            print("   1. Kafka is running on localhost:9092")
            print("   2. The topic 'user-activity-events' exists")
            print("   3. For Snappy compression: pip install python-snappy")
            return False
    
    def try_decode_message(self, message_bytes):
        """Try to decode message with different approaches"""
        try:
            # Try direct JSON decode first
            return json.loads(message_bytes.decode('utf-8'))
        except UnicodeDecodeError:
            try:
                # Try to handle potential compression
                print("Message appears to be compressed. Installing python-snappy may help.")
                return {"raw_message": "Compressed message - install python-snappy for full support"}
            except Exception:
                return {"raw_message": "Unable to decode message"}
        except json.JSONDecodeError:
            return {"raw_message": "Invalid JSON message"}
    
    def format_timestamp(self, timestamp_str: str) -> str:
        """Format timestamp for display"""
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            return timestamp_str
    
    def display_message(self, message):
        """Display a received message in a formatted way"""
        print("\n" + "="*80)
        print(f"MESSAGE RECEIVED at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # Message metadata
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Timestamp: {datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Message value (the actual event data)
        value = message.value
        
        # Check if it's a raw message (compression issue)
        if isinstance(value, dict) and "raw_message" in value:
            print(f"\nMESSAGE DECODE ISSUE:")
            print(f"   {value['raw_message']}")
            print(f"   Install python-snappy: pip install python-snappy")
            print("="*80)
            return
        
        print("\nEVENT DATA:")
        print("-" * 40)
        
        # Display basic event info
        print(f"Event ID: {value.get('event_id', 'N/A')}")
        print(f"Request ID: {value.get('request_id', 'N/A')}")
        print(f"Event Type: {value.get('event_type', 'N/A')}")
        print(f"User ID: {value.get('user_id', 'N/A')}")
        print(f"Session ID: {value.get('session_id', 'N/A')}")
        print(f"Page URL: {value.get('page_url', 'N/A')}")
        
        # Timestamps
        if 'timestamp' in value:
            print(f"Event Timestamp: {self.format_timestamp(value['timestamp'])}")
        
        # Client Info
        client_info = value.get('client_info', {})
        if client_info:
            print(f"\nCLIENT INFO:")
            print(f"   User Agent: {client_info.get('user_agent', 'N/A')}")
            print(f"   Screen Resolution: {client_info.get('screen_resolution', 'N/A')}")
            print(f"   Language: {client_info.get('language', 'N/A')}")
        
        # Service Info
        service_info = value.get('service_info', {})
        if service_info:
            print(f"\nSERVICE INFO:")
            print(f"   Service: {service_info.get('service_name', 'N/A')}")
            print(f"   Version: {service_info.get('service_version', 'N/A')}")
            print(f"   Environment: {service_info.get('environment', 'N/A')}")
        
        # Processing Info
        processing_info = value.get('processing_info', {})
        if processing_info:
            print(f"\nPROCESSING INFO:")
            if 'received_at' in processing_info:
                print(f"   Received At: {self.format_timestamp(processing_info['received_at'])}")
            if 'processed_at' in processing_info:
                print(f"   Processed At: {self.format_timestamp(processing_info['processed_at'])}")
            print(f"   Processing Time: {processing_info.get('processing_ms', 'N/A')}ms")
        
        # Event Data (custom data)
        event_data = value.get('event_data', {})
        if event_data:
            print(f"\nEVENT DATA:")
            for key, val in event_data.items():
                print(f"   {key}: {val}")
        
        print("="*80)
    
    def consume_messages(self):
        """Start consuming messages"""
        if not self.consumer:
            print("Consumer not initialized. Call connect() first.")
            return
        
        print("Starting to consume messages...")
        print("Send some events to the Ingestion-Service to see them here!")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                self.display_message(message)
                
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        except Exception as e:
            print(f"Error consuming messages: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close the consumer connection"""
        if self.consumer:
            self.consumer.close()
            print("Consumer connection closed.")

def main():
    """Main function"""
    print("Simple Kafka Consumer for Testing")
    print("=" * 50)
    
    parser = argparse.ArgumentParser(description="Control whether to read from beginning or latest")
    parser.add_argument("--read-from-beginning", action="store_true", help="Read messages from the beginning of the topic")
    args = parser.parse_args()
    
    consumer = SimpleKafkaConsumer(read_from_beginning=args.read_from_beginning)
    
    if consumer.connect():
        consumer.consume_messages()
    else:
        print("Failed to start consumer. Exiting.")
        sys.exit(1)

if __name__ == "__main__":
    main() 