package misc

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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var TW *tabwriter.Writer = GetNewWriter()

func GetNewWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 1, 0, 4, ' ', tabwriter.Debug)
}

func TabledOutput(t map[string]sarama.TopicDetail) {
	defer TW.Flush()
	// fmt.Fprintf(TW, "\n %s\t%s\t%s\t%s\t", "Topic Name", "Replication Factor", "Number Of Partitions", "Configuration Entries")
	// fmt.Fprintf(TW, "\n %s\t%s\t%s\t%s\t", "----------", "------------------", "--------------------", "---------------------")

	for topic, detail := range t {
		var cm string = ""
		for k, v := range detail.ConfigEntries {
			cm += fmt.Sprint(k, "=", *v, " , ")
		}
		fmt.Fprintf(TW, "\n %s\t%d\t%d\t%s", topic, detail.ReplicationFactor, detail.NumPartitions, cm)
	}
	fmt.Fprintln(TW, " ")
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if strings.ToLower(strings.TrimSpace(item)) == strings.ToLower(strings.TrimSpace(val)) {
			return i, true
		}
	}
	return -1, false
}

func FindSASLValues(s string, sep string) string {
	return strings.Split(strings.Split(strings.Replace(s, "'", "\"", -1), sep)[1], "\"")[1]
}

func RemoveValuesFromSlice(s []string, val string) []string {
	if pos, present := Find(s, val); present {
		for present {
			s = RemovePositionFromSlice(s, pos)
			pos, present = Find(s, val)
		}
	}
	return s
}

func RemovePositionFromSlice(s []string, i int) []string {
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

func ExistsInString(s string, part1 []string, part2 []string, sep string) bool {
	for _, v1 := range part1 {
		for _, v2 := range part2 {
			if strings.Contains(s, strings.Join([]string{v1, v2}, sep)) {
				return true
			}
		}
	}
	return false
}

func IsTopicName(topicName string, delimiter string) bool {
	if strings.HasSuffix(topicName, strings.Join([]string{delimiter, "*"}, "")) {
		return false
	} else {
		return true
	}
}

func InPlaceDedup(in []string) []string {
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
		fmt.Fprintf(TW, "\n%d\t%s", i, v)
	}
	fmt.Fprintln(TW, " ")
	TW.Flush()
}

func PrettyPrintMapSet(in mapset.Set) {
	defer TW.Flush()
	it := in.Iterator()
	index := 1
	for elem := range it.C {
		fmt.Fprintf(TW, "\n%d\t%s", index, elem)
		index += 1
	}
	fmt.Fprintln(TW, " ")
	it.Stop()
}

func GenerateRandomNumber(min int, max int) int {
	return rand.Intn(max-min) + min
}

func GenerateRandomDuration(interval int, unit string) time.Duration {
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

// This method sets up a zap logger object for use and returns back a pointer to the object.
func GetLogger(enableDebug *bool, enableConsole *bool) *zap.SugaredLogger {
	var config zap.Config
	if *enableConsole {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		config = zap.NewProductionConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		config.DisableStacktrace = true
		config.DisableCaller = true
	}
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder

	if *enableDebug {
		config.DisableStacktrace = false
		config.DisableCaller = false
	}

	logger, _ := config.Build()
	defer logger.Sync()
	return logger.Sugar()
}
