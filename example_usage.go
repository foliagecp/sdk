package main

import (
	"log"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db" // Adjust import path as needed
)

func main() {
	// Initialize CMDB client
	client, err := db.NewCMDBClient("nats://localhost:4222", 30, "hub")
	if err != nil {
		log.Fatalf("Failed to create CMDB client: %v", err)
	}

	// Example: Create two types and link them, then create objects
	if err := runCMDBExample(client); err != nil {
		log.Fatalf("CMDB example failed: %v", err)
	}

	log.Println("CMDB example completed successfully!")
}

func runCMDBExample(client db.CMDBClient) error {
	log.Println("=== Starting CMDB Example ===")

	// Step 1: Create Server type
	log.Println("1. Creating Server type...")
	serverTypeBody := easyjson.NewJSONObject()
	serverTypeBody.SetByPath("name", easyjson.NewJSON("Server"))
	serverTypeBody.SetByPath("description", easyjson.NewJSON("Physical or virtual server"))
	serverTypeBody.SetByPath("category", easyjson.NewJSON("infrastructure"))

	if err := client.TypeCreate("server", serverTypeBody); err != nil {
		return err
	}
	log.Println("✓ Server type created")

	// Step 2: Create Application type
	log.Println("2. Creating Application type...")
	appTypeBody := easyjson.NewJSONObject()
	appTypeBody.SetByPath("name", easyjson.NewJSON("Application"))
	appTypeBody.SetByPath("description", easyjson.NewJSON("Software application"))
	appTypeBody.SetByPath("category", easyjson.NewJSON("software"))

	if err := client.TypeCreate("application", appTypeBody); err != nil {
		return err
	}
	log.Println("✓ Application type created")

	// Step 3: Create link between types (Server -> Application)
	log.Println("3. Creating link between Server and Application types...")
	linkBody := easyjson.NewJSONObject()
	linkBody.SetByPath("relationship", easyjson.NewJSON("hosts"))
	linkBody.SetByPath("description", easyjson.NewJSON("Server hosts application"))

	if err := client.TypesLinkCreate("server", "application", "hosts", linkBody, []string{"hosting", "deployment"}); err != nil {
		return err
	}
	log.Println("✓ Types link created: Server -> Application")

	// Wait a moment for the operations to complete
	time.Sleep(1 * time.Second)

	// Step 4: Verify types were created
	log.Println("4. Verifying types...")
	serverData, err := client.TypeRead("server")
	if err != nil {
		return err
	}
	log.Printf("✓ Server type verified: %s", serverData.GetByPath("body.name").AsStringDefault(""))

	appData, err := client.TypeRead("application")
	if err != nil {
		return err
	}
	log.Printf("✓ Application type verified: %s", appData.GetByPath("body.name").AsStringDefault(""))

	// Step 5: Create Server objects
	log.Println("5. Creating Server objects...")

	// Create web server
	webServerBody := easyjson.NewJSONObject()
	webServerBody.SetByPath("hostname", easyjson.NewJSON("web-server-01"))
	webServerBody.SetByPath("ip_address", easyjson.NewJSON("192.168.1.10"))
	webServerBody.SetByPath("os", easyjson.NewJSON("Ubuntu 22.04"))
	webServerBody.SetByPath("cpu_cores", easyjson.NewJSON(4))
	webServerBody.SetByPath("memory_gb", easyjson.NewJSON(16))

	if err := client.ObjectCreate("web-server-01", "server", webServerBody); err != nil {
		return err
	}
	log.Println("✓ Web server object created")

	// Create database server
	dbServerBody := easyjson.NewJSONObject()
	dbServerBody.SetByPath("hostname", easyjson.NewJSON("db-server-01"))
	dbServerBody.SetByPath("ip_address", easyjson.NewJSON("192.168.1.20"))
	dbServerBody.SetByPath("os", easyjson.NewJSON("CentOS 8"))
	dbServerBody.SetByPath("cpu_cores", easyjson.NewJSON(8))
	dbServerBody.SetByPath("memory_gb", easyjson.NewJSON(32))

	if err := client.ObjectCreate("db-server-01", "server", dbServerBody); err != nil {
		return err
	}
	log.Println("✓ Database server object created")

	// Step 6: Create Application objects
	log.Println("6. Creating Application objects...")

	// Create web application
	webAppBody := easyjson.NewJSONObject()
	webAppBody.SetByPath("name", easyjson.NewJSON("E-commerce Website"))
	webAppBody.SetByPath("version", easyjson.NewJSON("2.1.0"))
	webAppBody.SetByPath("language", easyjson.NewJSON("Python"))
	webAppBody.SetByPath("framework", easyjson.NewJSON("Django"))
	webAppBody.SetByPath("port", easyjson.NewJSON(8080))

	if err := client.ObjectCreate("ecommerce-web", "application", webAppBody); err != nil {
		return err
	}
	log.Println("✓ Web application object created")

	// Create database application
	dbAppBody := easyjson.NewJSONObject()
	dbAppBody.SetByPath("name", easyjson.NewJSON("PostgreSQL Database"))
	dbAppBody.SetByPath("version", easyjson.NewJSON("14.2"))
	dbAppBody.SetByPath("type", easyjson.NewJSON("relational"))
	dbAppBody.SetByPath("port", easyjson.NewJSON(5432))

	if err := client.ObjectCreate("postgresql-db", "application", dbAppBody); err != nil {
		return err
	}
	log.Println("✓ Database application object created")

	// Step 7: Create links between objects (following the type relationship)
	log.Println("7. Creating links between objects...")

	// Link web server to web application
	webHostingBody := easyjson.NewJSONObject()
	webHostingBody.SetByPath("deployment_date", easyjson.NewJSON("2024-01-15"))
	webHostingBody.SetByPath("environment", easyjson.NewJSON("production"))
	webHostingBody.SetByPath("auto_restart", easyjson.NewJSON(true))

	if err := client.ObjectsLinkCreate("web-server-01", "ecommerce-web", "hosts", webHostingBody, []string{"production", "web"}); err != nil {
		return err
	}
	log.Println("✓ Web server -> Web application link created")

	// Link database server to database application
	dbHostingBody := easyjson.NewJSONObject()
	dbHostingBody.SetByPath("deployment_date", easyjson.NewJSON("2024-01-10"))
	dbHostingBody.SetByPath("environment", easyjson.NewJSON("production"))
	dbHostingBody.SetByPath("backup_enabled", easyjson.NewJSON(true))
	dbHostingBody.SetByPath("replication", easyjson.NewJSON("master"))

	if err := client.ObjectsLinkCreate("db-server-01", "postgresql-db", "hosts", dbHostingBody, []string{"production", "database"}); err != nil {
		return err
	}
	log.Println("✓ Database server -> Database application link created")

	// Step 8: Verify created objects
	log.Println("8. Verifying created objects...")

	webServerData, err := client.ObjectRead("web-server-01")
	if err != nil {
		return err
	}
	log.Printf("✓ Web server verified: %s (%s)",
		webServerData.GetByPath("body.hostname").AsStringDefault(""),
		webServerData.GetByPath("body.ip_address").AsStringDefault(""))

	webAppData, err := client.ObjectRead("ecommerce-web")
	if err != nil {
		return err
	}
	log.Printf("✓ Web application verified: %s v%s",
		webAppData.GetByPath("body.name").AsStringDefault(""),
		webAppData.GetByPath("body.version").AsStringDefault(""))

	// Step 9: Read object links to verify relationships
	log.Println("9. Verifying object relationships...")

	linkData, err := client.ObjectsLinkRead("web-server-01", "ecommerce-web")
	if err != nil {
		return err
	}
	log.Printf("✓ Web server link verified: %s environment",
		linkData.GetByPath("body.environment").AsStringDefault(""))

	return nil
}

// Alternative example using low-level graph operations
func runLowLevelExample(client db.CMDBClient) error {
	log.Println("=== Low-Level Graph Example ===")

	// Using inherited GraphSyncClient methods for low-level operations

	// Create a simple vertex
	vertexBody := easyjson.NewJSONObject()
	vertexBody.SetByPath("type", easyjson.NewJSON("test_vertex"))
	vertexBody.SetByPath("data", easyjson.NewJSON("sample data"))

	if err := client.VertexCreate("test-vertex-1", vertexBody); err != nil {
		return err
	}
	log.Println("✓ Test vertex created")

	// Create another vertex
	vertex2Body := easyjson.NewJSONObject()
	vertex2Body.SetByPath("type", easyjson.NewJSON("test_vertex_2"))
	vertex2Body.SetByPath("data", easyjson.NewJSON("sample data 2"))

	if err := client.VertexCreate("test-vertex-2", vertex2Body); err != nil {
		return err
	}
	log.Println("✓ Test vertex 2 created")

	// Create a link between vertices
	linkBody := easyjson.NewJSONObject()
	linkBody.SetByPath("weight", easyjson.NewJSON(1.5))
	linkBody.SetByPath("description", easyjson.NewJSON("test connection"))

	if err := client.VerticesLinkCreate("test-vertex-1", "test-vertex-2", "connects_to", "relationship", []string{"test"}, linkBody); err != nil {
		return err
	}
	log.Println("✓ Vertices link created")

	// Read vertex with details
	vertexData, err := client.VertexRead("test-vertex-1", true)
	if err != nil {
		return err
	}
	log.Printf("✓ Vertex data retrieved: type=%s", vertexData.GetByPath("body.type").AsStringDefault("unknown"))

	return nil
}
