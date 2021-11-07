package systemtest

import (
	"os"
	"strings"
	"testing"
)

// Checks if the env-variable to activate system test is set and returns a broker
// If system tests are not activated, will skip the test
func initSystemTest(t *testing.T) []string {
	if _, isIntegration := os.LookupEnv("GOKA_SYSTEMTEST"); !isIntegration {
		t.Skip("*** skip integration test ***")
	}
	if brokers, ok := os.LookupEnv("GOKA_SYSTEMTEST_BROKERS"); ok {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9092"}
}
