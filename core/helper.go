package kafkashepherd

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	mapset "github.com/deckarep/golang-set"
)

var tw tabwriter.Writer = *GetNewWriter()

func GetNewWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 1, 0, 4, ' ', tabwriter.Debug)
}

func TabledOutput(t map[string]sarama.TopicDetail) {
	defer tw.Flush()
	// fmt.Fprintf(&tw, "\n %s\t%s\t%s\t%s\t", "Topic Name", "Replication Factor", "Number Of Partitions", "Configuration Entries")
	// fmt.Fprintf(&tw, "\n %s\t%s\t%s\t%s\t", "----------", "------------------", "--------------------", "---------------------")

	for topic, detail := range t {
		var cm string = ""
		for k, v := range detail.ConfigEntries {
			cm += fmt.Sprint(k, "=", *v, " , ")
		}
		fmt.Fprintf(&tw, "\n %s\t%d\t%d\t%s", topic, detail.ReplicationFactor, detail.NumPartitions, cm)
	}
	fmt.Fprintln(&tw, " ")
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if strings.ToLower(strings.TrimSpace(item)) == strings.ToLower(strings.TrimSpace(val)) {
			return i, true
		}
	}
	return -1, false
}

func RemoveValuesFromSlice(s []string, val string) []string {
	if pos, present := Find(s, val); present {
		for present {
			s = removePositionFromSlice(s, pos)
			pos, present = Find(s, val)
		}
	}
	return s
}

func removePositionFromSlice(s []string, i int) []string {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func GetPermutationsInt(slices [][]int) [][]int {
	ret := [][]int{}

	if len(slices) == 0 {
		return ret
	}

	if len(slices) == 1 {
		for _, sl := range slices[0] {
			ret = append(ret, []int{sl})
		}
		return ret
	}

	t := GetPermutationsInt(slices[1:])
	for _, sl := range slices[0] {
		for _, perm := range t {
			toRetAdd := append([]int{sl}, perm...)
			ret = append(ret, toRetAdd)
		}
	}

	return ret
}

func GetPermutationsString(sSlices [][]string) [][]string {
	ret := [][]string{}

	if len(sSlices) == 0 {
		return ret
	}

	if len(sSlices) == 1 {
		for _, sSlice := range sSlices[0] {
			if strings.TrimSpace(sSlice) != "" {
				ret = append(ret, []string{sSlice})
			}
		}
		return ret
	}

	t := GetPermutationsString(sSlices[1:])
	for _, sSlice := range sSlices[0] {
		for _, perm := range t {
			toRetAdd := append([]string{sSlice}, perm...)
			ret = append(ret, toRetAdd)
		}
	}

	return ret
}

func IsZero1DSlice(s []string) bool {
	for _, v := range s {
		if v != "" {
			return false
		}
	}
	return true
}

func existsInString(s string, part1 []string, part2 []string, sep string) bool {
	for _, v1 := range part1 {
		for _, v2 := range part2 {
			if strings.Contains(s, strings.Join([]string{v1, v2}, sep)) {
				return true
			}
		}
	}
	return false
}

func isTopicName(topicName string, delimiter string) bool {
	if strings.HasSuffix(topicName, strings.Join([]string{delimiter, "*"}, "")) {
		return false
	} else {
		return true
	}
}

func inPlaceDedup(in []string) []string {
	sort.Strings(in)
	j := 0
	for i := 1; i < len(in); i++ {
		if in[j] == in[i] {
			continue
		}
		j++
		in[j] = in[i]
	}
	return in[:j+1]
}

func PrettyPrint1DStringSlice(in []string) {
	for i, v := range in {
		fmt.Fprintf(&tw, "\n%d\t%s", i, v)
	}
	fmt.Fprintln(&tw, " ")
	tw.Flush()
}

func PrettyPrintMapSet(in mapset.Set) {
	defer tw.Flush()
	it := in.Iterator()
	index := 1
	for elem := range it.C {
		fmt.Fprintf(&tw, "\n%d\t%s", index, elem)
		index += 1
	}
	fmt.Fprintln(&tw, " ")
	it.Stop()
}

func generateRandomNumber(min int, max int) int {
	return rand.Intn(max-min) + min
}

func generateRandomDuration(interval int, unit string) time.Duration {
	dur, err := time.ParseDuration(fmt.Sprint(interval, unit))
	if err != nil {
		fmt.Println("Could not generate Duration", err)
		os.Exit(1)
	}
	return dur
}

func DottedLineOutput(comment string, seperator string, length int) {
	right := (length - len(comment)) / 2
	left := right
	diff := length - (right + left + len(comment))
	if length > diff && math.Mod(float64(diff), 2) == float64(0) {
		right += diff / 2
		left += diff / 2
	}
	if length > diff && math.Mod(float64(diff), 2) == float64(1) {
		right += diff / 2
		left += (diff / 2) + 1
	}
	fmt.Println(strings.Repeat(seperator, length))
	fmt.Printf("%s%s%s\n", strings.Repeat(seperator, right), comment, strings.Repeat(seperator, left))
	fmt.Println(strings.Repeat(seperator, length))
}
