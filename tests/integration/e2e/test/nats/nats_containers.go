package nats

import (
	"context"
	"fmt"
	"github.com/foliagecp/sdk/statefun/logger"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"strconv"
)

// Each URL in a remote needs to point to the same cluster.
// If one node in a cluster is configured as leaf node, all nodes need to.
// Likewise, if one server in a cluster accepts leaf node connections, all servers need to.

const (
	natsImage       = "nats:2.10"
	clusterHttpPort = "8222"
	remotesLeafPort = "7422"
	serverPort      = "6222"
)

type Cluster struct {
	Containers []*Container
	Leaves     []*Container
}

var lg = logger.NewLogger(logger.Options{Level: logger.DebugLevel})

// CreateCluster initializes a NATS cluster with the configuration provided.
func CreateCluster(ctx context.Context, clusterName string, serverClusterQuantity int, leavesQuantity int) (*Cluster, error) {

	// CreateCluster a new docker network
	nwr, err := network.New(ctx)
	if err != nil {
		lg.Fatal(ctx, "Failed to create network: %s\nIs the Docker runtime started?", err)
		return nil, err
	}

	leafName := "leaf"

	// Generate NATS cluster routes
	routes := generateRoutes(clusterName, serverClusterQuantity)
	remotes := generateLeafRemotes(clusterName, serverClusterQuantity)

	leaves := make([]*Container, 0, serverClusterQuantity)
	servers := make([]*Container, 0, serverClusterQuantity)

	for i := 0; i < serverClusterQuantity; i++ {
		args := []testcontainers.ContainerCustomizer{
			// network specific to this nats (cluster{i})
			network.WithNetwork([]string{fmt.Sprintf("%s%d", clusterName, i)}, nwr),
			// cluster id ("cluster{i}")
			WithArgument("name", clusterName+strconv.Itoa(i)),
			// route to this specific nats - nats://cluster{i}:6222
			WithArgument("cluster", fmt.Sprintf("nats://%s%d:%s", clusterName, i, serverPort)),
			// same for each nats (cluster)
			WithArgument("cluster_name", clusterName),
			// combined routes to each server - predefined for all
			WithArgument("routes", routes),
			// cluster port, it's static 'cause of different networks used for each cluster
			WithArgument("http_port", clusterHttpPort),
			// config for leaf connections
			WithArgument("config", "server.conf"),
		}

		serverContainer, err := Run(ctx, natsImage, args...)

		if err != nil {
			lg.Fatal(ctx, "Failed to acquire serverContainer %d: %s", i, err)
		}
		err = serverContainer.CopyToContainer(
			ctx,
			[]byte(fmt.Sprintf("leafnodes { port : %s }", remotesLeafPort)),
			"/server.conf",
			700,
		)
		if err != nil {
			lg.Fatal(ctx, "Failed to copy config file %d: %s", i, err)
		}
		servers = append(servers, serverContainer)
	}

	for i := 0; i < leavesQuantity; i++ {
		// for each server in cluster create a leaf nats
		leafContainer, err := Run(ctx, natsImage,
			// network specific to this nats (cluster{i})
			network.WithNetwork([]string{fmt.Sprintf("%s%d", leafName, i)}, nwr),
			// cluster id ("cluster{i}")
			WithArgument("name", leafName+strconv.Itoa(i)),
			// cluster port, it's static 'cause of different networks used for each cluster
			WithArgument("http_port", clusterHttpPort),
			WithArgument("config", "leaf.conf"),
		)
		if err != nil {
			lg.Fatal(ctx, "Failed to start serverContainer %d: %s", i, err)
		}
		err = leafContainer.CopyToContainer(
			ctx,
			[]byte(fmt.Sprintf(`leafnodes { remotes = [ %s ] }`, remotes)),
			"/leaf.conf",
			700,
		)
		if err != nil {
			lg.Panic(ctx, "Failed to copy config file %d: %s", i, err)
		}
		leaves = append(leaves, leafContainer)
	}

	return &Cluster{
		Containers: servers,
		Leaves:     leaves,
	}, err
}

// GetClusterConnection returns a slice of connection strings of NATS clusters servers to connect to.
func (nc *Cluster) GetClusterConnection(ctx context.Context) []string {
	var connections = make([]string, 0, len(nc.Containers))
	for _, v := range nc.Containers {
		connections = append(connections, v.MustConnectionString(ctx))
	}
	return connections // Trim the trailing comma
}

// GetLeavesConnection returns a slice of connection strings of NATS leaf servers to connect to.
func (nc *Cluster) GetLeavesConnection(ctx context.Context) []string {
	var connections = make([]string, 0, len(nc.Leaves))
	for _, v := range nc.Leaves {
		connections = append(connections, v.MustConnectionString(ctx))
	}
	return connections
}

// Terminate terminates all containers within the cluster.
func (nc *Cluster) Terminate(ctx context.Context) {
	for _, v := range nc.Containers {
		if err := v.Terminate(ctx); err != nil {
			lg.Fatal(ctx, "failed to terminate container")
		}
	}
	for _, v := range nc.Leaves {
		if err := v.Terminate(ctx); err != nil {
			lg.Fatal(ctx, "failed to terminate leaf container")
		}
	}
}

// Start starts all containers within the cluster.
func (nc *Cluster) Start(ctx context.Context) {
	for _, v := range nc.Containers {
		if err := v.Start(ctx); err != nil {
			lg.Fatal(ctx, "failed to start container")
		}
	}
	for _, v := range nc.Leaves {
		if err := v.Start(ctx); err != nil {
			lg.Fatal(ctx, "failed to start leaf container")
		}
	}
}

// generateRoutes creates a comma-separated string of NATS server URLs for clustering.
func generateRoutes(name string, quantity int) string {
	var routes string
	for i := 0; i < quantity; i++ {
		routes += fmt.Sprintf("nats://%s%d:%s,", name, i, serverPort)
	}
	return routes[:len(routes)-1] // Trim the trailing comma
}

// generateLeafRemotes creates a comma-separated string of NATS server remotes for connecting leaves.
func generateLeafRemotes(name string, quantity int) string {
	var routes string
	for i := 0; i < quantity; i++ {
		routes += fmt.Sprintf(`{url : "nats://%s%d:%s"},`, name, i, remotesLeafPort)
	}
	return routes[:len(routes)-1] // Trim the trailing comma
}
