package cache

// CACHE_MODE — пресет поверх частных настроек, а не замена им: он задаёт, чему
// они равны по умолчанию, а названная в окружении частная настройка побеждает.
// Так развёртывание, уже передающее CACHE_TIERING, ведёт себя ровно как прежде.

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
		{"", false, false, false, "tree"},
		{"records", true, false, false, "records"},
		{"zstd", true, true, false, "zstd"},
		{"zstd-dict", true, true, true, "zstd-dict"},
		{"zstd+dict", true, true, true, "zstd-dict"},
	} {
		restore := SetCacheModeForTest(c.mode)
		require.Equal(t, c.tiering, tieringEnabled(), "режим %q: ярус", c.mode)
		require.Equal(t, c.compress, compressionEnabled(), "режим %q: сжатие", c.mode)
		require.Equal(t, c.dict, dictionaryEnabled(), "режим %q: словарь", c.mode)
		require.Equal(t, c.name, CacheMode(), "режим %q: как называется", c.mode)
		restore()
	}
}

func Test_CacheMode_UnknownFallsBackToTree(t *testing.T) {
	restore := SetCacheModeForTest("чепуха")
	defer restore()
	require.False(t, tieringEnabled(), "неизвестное значение обязано оставить дерево")
	require.Equal(t, "tree", CacheMode())
}

// Test_CacheMode_IndividualSettingWins — частная настройка сильнее пресета.
func Test_CacheMode_IndividualSettingWins(t *testing.T) {
	require.False(t, resolveBool("CACHE_TIERING_ABSENT_IN_ENV", "on", false),
		"без переменной берётся пресет")
	require.True(t, resolveBool("CACHE_TIERING_ABSENT_IN_ENV", "on", true),
		"без переменной берётся пресет")

	t.Setenv("CACHE_MODE_TEST_KNOB", "on")
	require.True(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", false),
		"названная переменная обязана перебить пресет")

	t.Setenv("CACHE_MODE_TEST_KNOB", "off")
	require.False(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", true),
		"названная переменная обязана перебить пресет и в обратную сторону")

	t.Setenv("CACHE_MODE_TEST_KNOB", "  ")
	require.True(t, resolveBool("CACHE_MODE_TEST_KNOB", "on", true),
		"пустое значение — это не выбор, берётся пресет")
}

// Test_CacheMode_Restores — вспомогательная функция возвращает всё как было.
func Test_CacheMode_Restores(t *testing.T) {
	before := CacheMode()
	restore := SetCacheModeForTest("zstd-dict")
	require.Equal(t, "zstd-dict", CacheMode())
	restore()
	require.Equal(t, before, CacheMode(), "режим не вернулся к исходному")
}

// Test_CacheMode_ZeroSamplesMeansNoDictionary — нулевая выборка это тоже
// «без словаря», и отчёт о режиме обязан это отражать.
func Test_CacheMode_ZeroSamplesMeansNoDictionary(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	require.True(t, dictionaryEnabled())
	require.Equal(t, "zstd-dict", CacheMode())

	prev := dictSampleLimit
	dictSampleLimit = 0
	defer func() { dictSampleLimit = prev }()

	require.False(t, dictionaryEnabled(), "учиться не на чем — словаря быть не должно")
	require.Equal(t, "zstd", CacheMode(), "отчёт о режиме обязан согласоваться с настройками")
}
