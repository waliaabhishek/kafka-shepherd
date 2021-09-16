package misc

import (
	"testing"

	"github.com/stretchr/testify/suite"
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
		s.Assert().ElementsMatch(c.out, out)
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
		s.Equal(c.out, out)
	}
}
