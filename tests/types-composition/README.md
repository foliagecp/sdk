## Usage

First, run the Python script to start listening for inventory data:
```
python3 egress_collector.py
```
The script will:

- Connect to NATS server at nats://nats:foliage@nats:4222
- Subscribe to inventory egress topics
- Wait for counter increments and inventory data

Once the script is running, trigger foliage function using NATS CLI on start object with tag
```
nats pub -s nats://nats:foliage@nats:4222 signal.hub.functions.tests.type_composition.collect_inventory_info.rack1 "{\"payload\":{\"tag\":\"tag1\"}}"
```

You can also trigger collection for other objects:
```aiignore
nats pub -s nats://nats:foliage@nats:4222 signal.hub.functions.tests.type_composition.collect_inventory_info.server1 "{\"payload\":{\"tag\":\"tag1\"}}"

nats pub -s nats://nats:foliage@nats:4222 signal.hub.functions.tests.type_composition.collect_inventory_info.datacenter1 "{\"payload\":{\"tag\":\"tag1\"}}"
```

## Output
The script generates a JSON file with the following structure
```aiignore
{
  "timestamp": "2025-08-19T10:30:00",
  "total_items": 25,
  "inventory_tree": {
    "hub/datacenter1": {
      "serial": "DC001",
      "children": {
        "hub/rack1": {
          "serial": "RACK001",
          "children": {
            "hub/server1": {
              "serial": "SE0001",
              "children": {
                "hub/cpu1": {
                  "serial": "CPU0001",
                  "children": {}
                }
              }
            }
          }
        }
      }
    }
  }
}
```