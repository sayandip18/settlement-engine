package payment_test

import (
	"os"
	"testing"

	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }
