import json
import asyncio
import datetime
import nats

NATS_URL = 'nats://nats:foliage@127.0.0.1:4222'
EGRESS_TOPIC = 'egress.functions.tests.type_composition.collect_inventory_info.>'
COUNTER_SUBJECT = 'egress.functions.tests.type_composition.collect_inventory_info.__counter'
WAIT_TIMEOUT = 5
nc = None

class EgressCollector:
    def __init__(self):
        self.inventory_tree = {}
        self.received_count = 0
        self.collected_items = {}
        self.pending_count = 0
        self.zero_time = None
        self.stop_event = asyncio.Event()

    def build_hierarchy(self, item_data):
        item_id = item_data.get("id", "")
        caller = item_data.get("caller", "")
        serial = item_data.get("serial", "")

        node = {
            "id": item_id,
            "serial": serial,
            "children": {}
        }

        self.collected_items[item_id] = item_data

        self.pending_count -= 1
        print(f"Pending count: {self.pending_count}")

        if self.pending_count <= 0 and self.zero_time is None:
            self.zero_time = asyncio.get_event_loop().time()
            print(f"Counter reached zero, starting {WAIT_TIMEOUT}s timer")

        if not caller or caller == "":
            self.inventory_tree[item_id] = node
        else:
            parent_node = self.find_node_by_id(caller, self.inventory_tree)
            if parent_node:
                if parent_node:
                    parent_node["children"][item_id] = node
                else:
                    self.inventory_tree[item_id] = node

    def find_node_by_id(self, node_id, tree):
        for key, node in tree.items():
            if key == node_id:
                return node
            found = self.find_node_by_id(node_id, node["children"])
            if found:
                return found
        return None

    def increment_counter(self):
        self.pending_count += 1
        self.zero_time = None
        print(f"Counter incremented: {self.pending_count}")

    def check_timeout(self):
        if self.zero_time and (asyncio.get_event_loop().time() - self.zero_time) >= WAIT_TIMEOUT:
            print(f"Timeout reached, stopping collection")
            self.stop_event.set()
            return True
        return False
    def reorganize_tree(self):
        items_to_reorganize = []

        for item_id, item_data in self.collected_items.items():
            caller = item_data.get("caller", "")
            if caller and caller != "" and item_id in self.inventory_tree:
                parent_node = self.find_node_by_id(caller, self.inventory_tree)
                if parent_node and item_id not in parent_node["children"]:
                    items_to_reorganize.append((item_id, caller))

        for item_id, caller in items_to_reorganize:
            if item_id in self.inventory_tree:
                node = self.inventory_tree[item_id]
                del self.inventory_tree[item_id]

                parent_node = self.find_node_by_id(caller, self.inventory_tree)
                if parent_node:
                    parent_node["children"][item_id] = node

    def save_to_file(self, filename=None):
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"inventory_tree_{timestamp}.json"

        self.reorganize_tree()

        output = {
            "timestamp": datetime.datetime.now().isoformat(),
            "total_items": self.received_count,
            "inventory_tree": self.inventory_tree
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        print(f"Inventory tree saved to: {filename}")
        return filename

    def print_tree(self, tree=None, level=0):
        if tree is None:
            tree = self.inventory_tree

        indent = "  " * level
        for key, node in tree.items():
            print(f"{indent}├─ {key} (serial: {node['serial']})")
            if node['children']:
                self.print_tree(node['children'], level + 1)

collector = EgressCollector()

async def start_subscriber():
    async def handler(msg):
        subject = msg.subject

        try:
            data = json.loads(msg.data.decode())

            if subject == COUNTER_SUBJECT:
                collector.increment_counter()
            else:
                collector.received_count += 1

                subject_parts = subject.split('.')
                if len(subject_parts) > 5:
                    item_id = '.'.join(subject_parts[5:])
                    data['id'] = item_id

                print(f"[#{collector.received_count}] {data}")
                collector.build_hierarchy(data)

        except json.JSONDecodeError as e:
            print(f"JSON Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    await nc.subscribe(EGRESS_TOPIC, cb=handler)
    print(f"Subscribed to {EGRESS_TOPIC}")
    print(f"Counter subject: {COUNTER_SUBJECT}")

async def connect_and_get_domain(nats_url=NATS_URL):
    nc = await nats.connect(nats_url)
    domain = nc._server_info.get('domain', 'hub')
    return nc, domain

async def main():
    global nc

    try:
        nc, domain = await connect_and_get_domain()
        print(f"Connected to {NATS_URL}")

        await start_subscriber()

        print("Waiting for counter increments and inventory data...")
        while not collector.stop_event.is_set():
            await asyncio.sleep(0.1)
            collector.check_timeout()

    except KeyboardInterrupt:
        print("Stopping...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if nc:
            await nc.close()

        if collector.received_count > 0:
            print(f"\nCollected {collector.received_count} items")
            collector.print_tree()
            filename = collector.save_to_file()
        else:
            print("No data collected")

if __name__ == "__main__":
    asyncio.run(main())