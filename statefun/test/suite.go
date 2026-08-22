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
	// Before the runtime goes down: every mark that was opened must be closed.
	s.requireCacheQuiesced(s.T())
	s.Stop()
}
