package misc

import (
	"testing"

	mapset "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
)

var _ = func() bool {
	testing.Init()
	// os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", "./../configs/shepherd.yaml")
	// os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./../configs/blueprints.yaml")
	// os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", "./../configs/definitions_dev.yaml")
	return true
}()

type StackSuite struct {
	suite.Suite
}

func TestStackSuite(t *testing.T) {
	suite.Run(t, new(StackSuite))
}

func (s *StackSuite) TestStackSuite_Misc_Find() {
	in := []string{"test1", "test2", "test3", "test1"}

	cases := []struct {
		value string
		loc   int
		found bool
	}{
		{"test1", 0, true},
		{"test2", 1, true},
		{"test3", 2, true},
		{"test4", -1, false},
		{"TESt2", 1, true},
	}

	for _, c := range cases {
		loc, found := Find(&in, c.value)
		s.Equal(c.loc, loc, "Item not found in the right location")
		s.Equal(c.found, found, "Found bool is incorrect")
	}
}

func (s *StackSuite) TestStackSuite_Misc_RemoveValuesFromSlice() {
	cases := []struct {
		in  []string
		val string
		out []string
	}{
		{[]string{"test1", "test2", "test3", "test1"},
			"test1",
			[]string{"test2", "test3"}},
		{[]string{"test1", "test2", "test3", "test4"},
			"test2",
			[]string{"test1", "test3", "test4"}},
		{[]string{"test1", "test2", "test3", "test4"},
			"test4",
			[]string{"test1", "test3", "test2"}},
		{[]string{"test1", "test2", "test3", "test4"},
			"test1",
			[]string{"test2", "test4", "test3"}},
	}

	for _, c := range cases {
		out := RemoveValuesFromSlice(c.in, c.val)
		s.ElementsMatch(c.out, out)
	}
}

func (s *StackSuite) TestStackSuite_Misc_FindSASLValues() {
	cases := []struct {
		in  string
		sep string
		out string
	}{
		{"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"test-secret\"",
			"username", "admin"},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"test-secret\"",
			"password", "test-secret"},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"admin\" \npassword=\"test-secret\"",
			"username", "admin"},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"admin\" \npassword=\"test-secret\"",
			"password", "test-secret"},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"\" \npassword=\"test-secret\"",
			"username", ""},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"admin\" \npassword=\"test\"",
			"password", "test"},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"admin\" \npassword=\"\"",
			"password", ""},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"\" \npassword=\"\"",
			"username", ""},
		{"org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"\" \npassword=\"\"",
			"password", ""},
	}

	for _, c := range cases {
		out := FindSASLValues(c.in, c.sep)
		s.Equal(c.out, out)
	}
}

func (s *StackSuite) TestStackSuite_Misc_GetPermutationsInt() {
	cases := []struct {
		in     [][]int
		out    [][]int
		errMsg string
	}{
		{[][]int{}, [][]int{}, "Empty Use Case"},
		{[][]int{{1, 2, 3}}, [][]int{{1}, {2}, {3}}, "Only one []int failed"},
		{[][]int{{1, 2, 3}, {4}}, [][]int{{1, 4}, {2, 4}, {3, 4}}, "one with 3 elements and the second with 1 failed "},
		{[][]int{{1}, {2, 3, 4}}, [][]int{{1, 2}, {1, 3}, {1, 4}}, "one with 1 elements and the second with 3 failed "},
		// TODO: Fix the empty struct use cases .... this might be causing trouble with actual implementation.
		// {[][]int{{1, 2}, {3}, {}}, [][]int{{1, 3}, {2, 3}}, "empty last []int failed"},
		// {[][]int{{1, 2}, {}, {3}}, [][]int{{1, 3}, {2, 3}}, "empty second []int failed"},
		{[][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			[][]int{{1, 4, 7}, {1, 4, 8}, {1, 4, 9}, {1, 5, 7}, {1, 5, 8}, {1, 5, 9}, {1, 6, 7}, {1, 6, 8}, {1, 6, 9},
				{2, 4, 7}, {2, 4, 8}, {2, 4, 9}, {2, 5, 7}, {2, 5, 8}, {2, 5, 9}, {2, 6, 7}, {2, 6, 8}, {2, 6, 9},
				{3, 4, 7}, {3, 4, 8}, {3, 4, 9}, {3, 5, 7}, {3, 5, 8}, {3, 5, 9}, {3, 6, 7}, {3, 6, 8}, {3, 6, 9},
			}, "Multi Element per []int failed"},
	}

	for _, c := range cases {
		out := GetPermutationsInt(c.in)
		s.ElementsMatch(c.out, out, c.errMsg)
	}
}

func (s *StackSuite) TestStackSuite_Misc_GetPermutationsString() {
	cases := []struct {
		in     [][]string
		out    [][]string
		errMsg string
	}{
		{[][]string{}, [][]string{}, "Empty Use Case"},
		{[][]string{{"1", "2", "3"}}, [][]string{{"1"}, {"2"}, {"3"}}, "Only one []string failed"},
		{[][]string{{"1", "2", "3"}, {"4"}}, [][]string{{"1", "4"}, {"2", "4"}, {"3", "4"}}, "one with 3 elements and the second with 1 failed "},
		{[][]string{{"1"}, {"2", "3", "4"}}, [][]string{{"1", "2"}, {"1", "3"}, {"1", "4"}}, "one with 1 elements and the second with 3 failed "},
		// TODO: Fix the empty struct use cases .... this might be causing trouble with actual implementation.
		// {[][]string{{"1", "2"}, {"3"}, {""}}, [][]string{{"1", "3"}, {"2", "3"}}, "empty last []string failed"},
		// {[][]string{{"1", "2"}, {}, {"3"}}, [][]string{{"1", "3"}, {"2", "3"}}, "empty second []string failed"},
		{[][]string{{"1", "2", "3"}, {"4", "5", "6"}, {"7", "8", "9"}},
			[][]string{{"1", "4", "7"}, {"1", "4", "8"}, {"1", "4", "9"}, {"1", "5", "7"}, {"1", "5", "8"}, {"1", "5", "9"}, {"1", "6", "7"}, {"1", "6", "8"}, {"1", "6", "9"},
				{"2", "4", "7"}, {"2", "4", "8"}, {"2", "4", "9"}, {"2", "5", "7"}, {"2", "5", "8"}, {"2", "5", "9"}, {"2", "6", "7"}, {"2", "6", "8"}, {"2", "6", "9"},
				{"3", "4", "7"}, {"3", "4", "8"}, {"3", "4", "9"}, {"3", "5", "7"}, {"3", "5", "8"}, {"3", "5", "9"}, {"3", "6", "7"}, {"3", "6", "8"}, {"3", "6", "9"},
			}, "Multi Element per []string failed"},
	}

	for _, c := range cases {
		out := GetPermutationsString(c.in)
		s.ElementsMatch(c.out, out, c.errMsg)
	}
}

func (s *StackSuite) TestStackSuite_Misc_IsZero1DSlice() {
	cases := []struct {
		in  []string
		out bool
		err string
	}{
		{[]string{"test"}, false, "Failed the single item slice case"},
		{[]string{""}, true, "Failed the empty string item slice case"},
		{[]string{}, true, "Failed the empty slice item slice case"},
	}

	for _, c := range cases {
		out := IsZero1DSlice(c.in)
		s.Equal(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_ExistsInString() {
	cases := []struct {
		in    string
		srch1 []string
		srch2 []string
		sep   string
		out   bool
		err   string
	}{
		{"test0.test2.test4.test3", []string{"test1", "test2"}, []string{"test3", "test4"}, ".", true, "concat value success search case"},
		{"test0.test2.test1.test5", []string{"test1", "test2"}, []string{"test3", "test4"}, ".", false, "concat value failure search case"},
		{"", []string{"test1", "test2"}, []string{"test3", "test4"}, ".", false, "empty value search case"},
	}

	for _, c := range cases {
		out := ExistsInString(c.in, c.srch1, c.srch2, c.sep)
		s.Equal(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_IsTopicName() {
	cases := []struct {
		in  string
		sep string
		out bool
		err string
	}{
		{"test", ".", true, "Full Topic name case"},
		{"test.test2", ".", true, "Full Topic name case"},
		{"", ".", false, "Full Topic name case"},
		{"*", ".", false, "Full Topic name case"},
		{"test.*", ".", false, "Regex topic name success case"},
		{"test**", ".", false, "Regex topic name failure case"},
	}

	for _, c := range cases {
		out := IsTopicName(c.in, c.sep)
		s.Equal(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_InPlaceDedup() {
	cases := []struct {
		in  []string
		out []string
		err string
	}{
		{[]string{"1", "2", "3"}, []string{"1", "2", "3"}, "no dupes slice case"},
		{[]string{}, []string{}, "empty slice case"},
		{[]string{"1", "2", "3", "1"}, []string{"1", "2", "3"}, "dupe slice case"},
		{[]string{"1", "2", "3", "1", "2"}, []string{"1", "2", "3"}, "multi dupes slice case"},
		{[]string{"1", "2", "3", "1", "2", "2", "2"}, []string{"2", "1", "3"}, "multi dupes slice case 2"},
	}

	for _, c := range cases {
		out := InPlaceDedup(c.in)
		s.ElementsMatch(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_GenerateRandomNumber() {
	cases := []struct {
		min int
		max int
		out int
		err string
	}{
		{0, 10, 10, "positive numbers case"},
		{-10, 0, 0, "negative numbers case"},
		{-1, 10, 10, "negative & positive numbers case"},
	}

	for _, c := range cases {
		out := GenerateRandomNumber(c.min, c.max)
		s.GreaterOrEqual(out, c.min, "Number less than Minimum. "+c.err)
		s.LessOrEqual(out, c.max, "Number greater than Maximum. "+c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_GetLogger() {
	cases := []struct {
		isDebug      bool
		isStructured bool
		err          string
	}{
		{false, false, "both false use case"},
		{false, true, "false true use case"},
		{true, false, "true false use case"},
		{true, true, "true true use case"},
	}

	for _, c := range cases {
		logger := GetLogger(c.isDebug, c.isStructured)
		s.Equal(c.isDebug, logger.Desugar().Core().Enabled(zapcore.DebugLevel), c.err)
		// TODO: figure out a way to check the structured status
	}

}

func (s *StackSuite) TestStackSuite_Misc_GetStringSliceFromMapSet() {
	cases := []struct {
		in  mapset.Set
		out []string
		err string
	}{
		{mapset.NewSetWith("A", "B", "C", "D"), []string{"A", "B", "C", "D"}, "populated MapSet test"},
		{mapset.NewSetWith("A"), []string{"A"}, "Single Element MapSet test"},
		{mapset.NewSetWith(""), []string{""}, "Single zero value Element MapSet test"},
		{mapset.NewSet(), []string{}, "nil value Element MapSet test"},
	}

	for _, c := range cases {
		out := GetStringSliceFromMapSet(c.in)
		s.ElementsMatch(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_Misc_GetMapSetFromStringSlice() {
	cases := []struct {
		out mapset.Set
		in  []string
		err string
	}{
		{mapset.NewSetWith("A", "B", "C", "D"), []string{"A", "B", "C", "D"}, "populated MapSet test"},
		{mapset.NewSetWith("A"), []string{"A"}, "Single Element MapSet test"},
		{mapset.NewSetWith(""), []string{""}, "Single zero value Element MapSet test"},
		{mapset.NewSet(), []string{}, "nil value Element MapSet test"},
	}

	for _, c := range cases {
		out := GetMapSetFromStringSlice(&c.in)
		s.ElementsMatch(c.out.ToSlice(), (*out).ToSlice(), c.err)
	}
}
