# How to write an application

1. **Define Objects:** Begin by clearly defining the objects with which your application will work.

2. **Populate the Graph Database:**
   - You can create these objects directly within your designed application, for example, during the bootstrap phase.
   - Alternatively, you can import objects from another Foliage application using an adapter.

3. **Develop Foliage Stateful Functions:**
   - Create all the stateful functions that will form the core of your application.

4. **Implement Asynchronous Communication:**
   - Organize these functions to communicate asynchronously using signals, which are handled by NATS in your preferred manner.
   - Utilize Foliage Statefun's context to store data between these calls.
   - Also, consider using an object's context for managing relevant information.

## Example of a test application for json template based WebUI

https://github.com/foliagecp/foliage-nats-test-statefun/blob/fix/ui-stub/ui_client.go