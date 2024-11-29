package test

import "github.com/stretchr/testify/suite"

type StatefunTestSuite struct {
	suite.Suite
	*statefunTestEnvironment
}

func (s *StatefunTestSuite) SetupTest() {
	s.statefunTestEnvironment = newStatefunTestEnvironment()
}

func (s *StatefunTestSuite) AfterTest(suiteName, testName string) {
	s.Stop()
}
