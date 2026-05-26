import uuid
import random
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Initial Faker
Faker.seed(0)
fake = Faker("en_US")

CATEGORIES = ['Electronics', 'Fashion', 'Home & Garden', 'Sports', 'Toys']
DEVICE_CATEGORY = ['mobile', 'desktop', 'tablet']
OS_BROWSER = ['IOS', 'Chrome', 'Brave', 'Edge', 'Android']
UTM_SOURCE = ['Google', 'Facebook', 'TikTok', 'direct', 'email']

# Create product

CATALOG = [
    {
        "product_id": f"PRD-{str(i).zfill(4)}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(10.0, 500.0), 2)
    }
    
    for i in range (1, 51)
]

@dataclass
class EventClick:
    event_id: str
    event_timestamp: int
    event_name: str
    page_url: str

@dataclass
class Product:
    product_id: str
    category: str
    price: float
    quantity: int
    transaction_id: str = None

@dataclass
class UserSession:
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    client_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = field(default_factory=lambda: str(fake.unique.random_int(min=111111, max=999999)))
    ip_address: str = field(default_factory=lambda: str(fake.ipv4()))
    device_category: str = field(default_factory=lambda: str(random.choice(DEVICE_CATEGORY)))
    os_browser: str = field(default_factory=lambda: str(random.choice(OS_BROWSER)))
    utm_source: str = field(default_factory=lambda: str(random.choice(UTM_SOURCE)))

    current_state: str = "page_view"
    cart_total: float = 0.0
    is_active: bool = True
    current_product: dict = field(default=None, repr=False)

    def get_next_event(self):
        if not self.is_active:
            return None
        
        event = EventClick(
            event_id = str(uuid.uuid4()),
            event_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000),
            event_name = self.current_state,
            page_url = f"https://e-commerce-clickstream/{self.current_state}"
        )

        # Create project if in view_item or buy_item
        product_obj = None
        if self.current_state in ["view_item", "add_to_cart", "begin_checkout", "purchase"]:
            if not self.current_product or self.current_state == "view_item":
                self.current_product = random.choice(CATALOG)

            qty = random.randint(1,3) if self.current_state == "add_to_cart" else 1

            # Sum money into cart
            if self.current_state == "add_to_cart":
                self.cart_total += self.current_product["price"] * qty

            # Create transaction id
            txn_id = f"TXN-{fake.random_int(100000, 999999)}" if self.current_state == "purchase" else None

            product_obj = Product(
                product_id=self.current_product["product_id"],
                category=self.current_product["category"],
                price=self.current_product["price"],
                quantity=qty,
                transaction_id=txn_id
            )

        payload = self._merge_payload(event, product_obj)

        self._advance_state()

        return payload
    
    def _advance_state(self):
        if self.current_state == "page_view":
            self.current_state = random.choices(["view_item", "drop"], weights=[75,25])[0]
        elif self.current_state == "view_item":
            self.current_state = random.choices(["add_to_cart", "view_item", "drop"], weights=[20, 50, 30])[0]
        elif self.current_state == "add_to_cart":
            self.current_state = random.choices(["begin_checkout", "view_item", "drop"], weights=[40, 30, 30])[0]
        elif self.current_state == "begin_checkout":
            self.current_state = random.choices(["purchase", "drop"], weights=[20, 80])[0]
        elif self.current_state == "purchase":
            self.current_state = random.choice(["view_item", "drop"])
        

        if self.current_state == "drop":
            self.is_active = False
    
    def _merge_payload(self, event: EventClick, product: Product) -> dict:
        # Extract UserSession, EventClick
        payload = {**asdict(self), ** asdict(event)}

        # Clean internal variable
        for key in ["is_active", "current_state", "current_product"]:
            payload.pop(key, None)

        payload["cart_total"] = round(payload["cart_total"], 2)

        if product:
            payload.update({k: v for k, v in asdict(product).items() if v is not None})

        # Add wrong data into streaming 
        chaos_chance = random.randint(1, 100)
        if chaos_chance == 10:
            payload["user_id"] = None

        elif chaos_chance == 15:
            payload["cart_total"] = "error"
           
        return payload
    
if __name__ == "__main__":

    try:
        sr_client = SchemaRegistryClient({'url': 'http://localhost:18081'})
        schema_str = """
            {
            "type": "record",
            "name": "ClickstreamEvent",
            "namespace": "com.ecommerce.events",
            "fields": [
                {"name": "session_id", "type": ["null", "string"], "default": null},
                {"name": "client_id", "type": ["null", "string"], "default": null},
                {"name": "user_id", "type": ["null", "string"], "default": null},
                {"name": "ip_address", "type": ["null", "string"], "default": null},
                {"name": "device_category", "type": ["null", "string"], "default": null},
                {"name": "os_browser", "type": ["null", "string"], "default": null},
                {"name": "utm_source", "type": ["null", "string"], "default": null},
                {"name": "cart_total", "type": ["null", "float"], "default": null},
                {"name": "event_id", "type": "string"},
                {"name": "event_timestamp", "type": "long"},
                {"name": "event_name", "type": "string"},
                {"name": "page_url", "type": ["null", "string"], "default": null},
                {"name": "product_id", "type": ["null", "string"], "default": null},
                {"name": "category", "type": ["null", "string"], "default": null},
                {"name": "price", "type": ["null", "float"], "default": null},
                {"name": "quantity", "type": ["null", "int"], "default": null},
                {"name": "transaction_id", "type": ["null", "string"], "default": null}
            ]
            }
        """
        arvo_serializer = AvroSerializer(sr_client, schema_str)
        producer = SerializingProducer({
            'bootstrap.servers': 'localhost:19092',
            'value.serializer': arvo_serializer
        })
        TOPIC_NAME = 'ecommerce_clickstream'

    except Exception as e:
        print(f"Error kafka: {e}")
        exit(1)
    
    active_sessions = []

    try:
        count = 0
        while True:
            while len(active_sessions) < 100:
                active_sessions.append(UserSession())

            session = random.choice(active_sessions)
            event_data = session.get_next_event()

            if event_data:
                try:
                    producer.produce(topic=TOPIC_NAME, value=event_data)
                    producer.poll(0)
                except Exception as e:
                    with open("failed_events.log", "a") as f:
                        f.write(f"Failed to produce: {event_data} | Error: {str(e)}\n")
                    print("Error data! Write into failed_eveents.log, pipeline continue run")

                count += 1

                time_str = datetime.fromtimestamp(event_data['event_timestamp'] / 1000.0).strftime('%H:%M:%S')

                # print(f"[{time_str}] {event_data['event_name'].ljust(15)} | "
                #       f"User: {event_data['user_id']} | Cart: ${event_data['cart_total']} | "
                #       f"Item: {event_data.get('product_id', 'N/A')}")
                
                if count % 1000 == 0:
                    print(f"Send: {count} events...")

            active_sessions = [s for s in active_sessions if s.is_active]

    except KeyboardInterrupt:
        print("Send data clickstream is stop!")

    finally:
        if 'producer' in locals() and producer:
            producer.flush()
            producer.close()
