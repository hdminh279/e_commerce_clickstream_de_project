import json
import uuid
import random
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from faker import Faker
from kafka import KafkaProducer

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
        
        return payload
    
if __name__ == "__main__":

    try:
        server = 'localhost:9092'
        producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=32768,
            linger_ms=100
        )
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
                producer.send(topic=TOPIC_NAME, value=event_data)
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
