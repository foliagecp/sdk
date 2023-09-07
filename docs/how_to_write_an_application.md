# How to write an application
Do the following steps to write your own application:
1. Define objects the designed application is working with  
2. Put objects in the graph database
    - Create directly via the designed application (for e.g. once at the bootstrap)
    - Import via another foliage application (adapter)
3. Create all foliage stateful functions the designed application consists from
4. Organize them through the asynchronous calls (signals) handled by NATS in a the desired way
    - Use statefun's context to store data between the calls
    - Use object's context

## Example of a test application for json template based WebUI
https://github.com/foliagecp/foliage-nats-test-statefun/blob/fix/ui-stub/ui_client.go