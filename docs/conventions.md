# Conventions
## Graph schema convention
Graph data about a system should be stored in a uniform way as shown on the picture below.
![Alt text](./pics/GraphSchemaConvention.jpg)

### 1. Vertex `root`
Vertice's id=`root`. Always exists. The root entrypoint into the graph. Always has only 2 out links:
1. Link of type `__types` leading to vertex with id=`types`
2. Link of type `__objects` leading to vertex with id=`types`  
No body required.

### 2. Vertex `types`
Vertice's id=`types`. Has out links typed `__type` to all vertices representing object types (type-vertex). Each such link should have a tag `name_<to_type_name>`, where `to_type_name` is the name of a type the link is leading to.  
No body required.

### 3. Type-vertex `group`
Only exists if grouping is planned. Represents the scpecial `group` type. Has out links typed `__object` to all object-vertices representing a group. Always has link to itself representing the ability of objects of type `group` to connect with each other (grouping another groups). May have out links to other type-vertices.  
May have body that declares meta info and rules for group objects.

### 4. Type-vertex
For e.g. `server`, `disk`, `group` from the picture above.  
Vertex that contains all info about a type it represents. Has out links typed `__object` to all object-vertices of its type. May have out links to other type-vertices.  
May have body that declares meta info and rules for such typed objects.

### 5. Vertex `objects`
Each vertex with an ID of `objects` has outgoing links of type `__object` to all vertices representing objects (object-vertices). Each of these links should be tagged as `name_<to_object_name>`, where `to_object_name` corresponds to the name of the object to which the link leads. No additional data body is necessary.

### 6. Object-vertex `nav`
This vertex only exists if topology navigation is intended. It represents a special object of the `group` type. It has outgoing links to all object-vertices of the `group` type (as defined in the body of the link `group->group`), which collectively represent topology entry points. Additionally, it always has a link to a type-vertex of `group`. This vertex may also have outgoing links to other type-vertices. It serves as the entry point for topology navigation through JPGQL.

### 4. Object-vertex
For example, consider `server1`, `disk1`, and `nav` from the picture above. These vertices contain all the information about the objects they represent. Each of them has an outgoing link of type `__type` to a type-vertex that represents their respective types. Additionally, they may have outgoing links to other object-vertices. The type of a link from object-vertex `A` to object-vertex `B` should be defined in the body of a link from type-vertex `TypeA` to type-vertex `TypeB`. The body of each object-vertex contains all the necessary information about the object it represents.