package cache

// CACHE_MODE is a preset over the individual settings, not a replacement for
// them: it sets what they default to, and an individual setting named in the
// environment wins. So a deployment already passing CACHE_TIERING behaves
// exactly as before.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_CacheMode_Presets(t *testing.T) {
	for _, c := range []struct {
		mode              string
		tiering, compress bool
		dict              bool
		name              string
	}{
		{"tree", false, false, false, "tree"},
		// Empty is not a choice of the tree but the absence of a choice: the
		// same as not setting the variable at all, and the default is records.
		{"", true, false, false, "records"},
		{"records", true, false, false, "records"},
		{"zstd", true, true, false, "zstd"},
		{"zstd-dict", true, true, true, "zstd-dict"},
		{"zstd+dict", true, true, true, "zstd-dict"},
	} {
		restore := SetCacheModeForTest(c.mode)
		require.Equal(t, c.tiering, tieringEnabled(), "mode %q: tiering", c.mode)
		require.Equal(t, c.compress, compressionEnabled(), "mode %q: compression", c.mode)
		require.Equal(t, c.dict, dictionaryEnabled(), "mode %q: dictionary", c.mode)
		require.Equal(t, c.name, CacheMode(), "mode %q: reported name", c.mode)
		restore()
	}
}

func Test_CacheMode_UnknownFallsBackToTree(t *testing.T) {
	restore := SetCacheModeForTest("чепуха")
	defer restore()
	require.False(t, tieringEnabled(), "an unknown value must keep the tree")
	require.Equal(t, "tree", CacheMode())
}

// Test_CacheMode_IndividualSettingWins — an individual setting is stronger
// than the preset.
func Test_CacheMode_IndividualSettingWins(t *testing.T) {
	require.False(t, resolveBool("CACHE_TIERING_ABSENT_IN_ENV", "on", false),
		"without the variable the preset applies")
	require.True(t, resolveBool("CACHE_TIERING_ABSENT_IN_ENV", "on", true),
		"without the variable the preset applies")

	t.Setenv("CACHE_MODE_TEST_KNOB", "on")
	require.True(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", false),
		"a variable set in the environment must override the preset")

	t.Setenv("CACHE_MODE_TEST_KNOB", "off")
	require.False(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", true),
		"a variable set in the environment must override the preset in the other direction too")

	t.Setenv("CACHE_MODE_TEST_KNOB", "  ")
	require.True(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", true),
		"an empty value is not a choice — the preset applies")
}

// Test_CacheMode_Restores — the helper puts everything back as it was.
func Test_CacheMode_Restores(t *testing.T) {
	before := CacheMode()
	restore := SetCacheModeForTest("zstd-dict")
	require.Equal(t, "zstd-dict", CacheMode())
	restore()
	require.Equal(t, before, CacheMode(), "the mode did not return to the original")
}

// Test_CacheMode_ZeroSamplesMeansNoDictionary — a zero sample also means
// "no dictionary", and the reported mode must reflect that.
func Test_CacheMode_ZeroSamplesMeansNoDictionary(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	require.True(t, dictionaryEnabled())
	require.Equal(t, "zstd-dict", CacheMode())

	prev := dictSampleLimit
	dictSampleLimit = 0
	defer func() { dictSampleLimit = prev }()

	require.False(t, dictionaryEnabled(), "nothing to train on — there must be no dictionary")
	require.Equal(t, "zstd", CacheMode(), "the reported mode must agree with the settings")
}

// Test_CacheMode_DefaultIsRecords — the default is pinned by a test: it
// cannot be changed silently, and it is exactly what decides what a
// deployment that configured nothing gets.
func Test_CacheMode_DefaultIsRecords(t *testing.T) {
	require.Equal(t, modeRecords, parseCacheMode(defaultCacheMode),
		"the default must be the records mode")
	require.Equal(t, modeRecords, parseCacheMode(""),
		"an unset variable must give the same as the default")

	restore := SetCacheModeForTest(defaultCacheMode)
	defer restore()
	require.True(t, tieringEnabled(), "by default vertices must be stored as records")
	require.False(t, compressionEnabled(), "compression is off by default: it makes filling the cache twice as expensive")
	require.Equal(t, "records", CacheMode())

	// And the tree must stay one step away from here.
	back := SetCacheModeForTest("tree")
	require.False(t, tieringEnabled(), "CACHE_MODE=tree must bring back the previous behavior")
	back()
}
