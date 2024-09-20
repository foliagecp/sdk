package nats

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultClientPort     = "4222/tcp"
	defaultRoutingPort    = "6222/tcp"
	defaultMonitoringPort = "8222/tcp"

	waitForLog = "Listening for client connections on 0.0.0.0:4222"
)

// Container represents the NATS container type used in the module
type Container struct {
	testcontainers.Container
	User     string
	Password string
}

// Run creates an instance of the NATS container type
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{defaultClientPort, defaultRoutingPort, defaultMonitoringPort},
		Cmd:          []string{"-DV", "-js"},
		WaitingFor:   wait.ForLog(waitForLog),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          false, // only start after we are ready
	}

	// Gather all config options (defaults and then apply provided options)
	settings := defaultOptions()
	for _, opt := range opts {
		if apply, ok := opt.(CmdOption); ok {
			apply(&settings)
		}
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}

	// Include the command line arguments
	for k, v := range settings.CmdArgs {
		// always prepend the dash because it was removed in the options
		genericContainerReq.Cmd = append(genericContainerReq.Cmd, []string{"--" + k, v}...)
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	natsContainer := Container{
		Container: container,
		User:      settings.CmdArgs["user"],
		Password:  settings.CmdArgs["pass"],
	}

	return &natsContainer, nil
}

func (c *Container) MustConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, args...)
	if err != nil {
		panic(err)
	}
	return addr
}

// ConnectionString returns a connection string for the NATS container
func (c *Container) ConnectionString(ctx context.Context, args ...string) (string, error) {
	mappedPort, err := c.MappedPort(ctx, defaultClientPort)
	if err != nil {
		return "", err
	}

	hostIP, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	uri := fmt.Sprintf("nats://%s:%s", hostIP, mappedPort.Port())
	return uri, nil
}
