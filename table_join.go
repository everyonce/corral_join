package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bcongdon/corral"
	log "github.com/sirupsen/logrus"
)

type finalOutputRecord struct {
	id         string
	title      string
	firstActor string
}

type wordCount struct{}

func (w wordCount) Map(key, value string, emitter corral.Emitter) {
	var id, newoutput string
	r := csv.NewReader(strings.NewReader(value))
	r.LazyQuotes = true
	line, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}
	switch len(line) {
	case 24: // Movie Info Record
		id = line[5]
		newoutput = "MOVIE_" + value
	case 3:
		id = line[2]
		newoutput = "CREDITS_" + value
	default:
		id = "0"
		newoutput = "UNKNOWN FILE TYPE: " + value
	}
	err = emitter.Emit(id, newoutput)
	if err != nil {
		fmt.Println(err)
	}
}

func (w wordCount) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	var x finalOutputRecord
	for value := range values.Iter() {
		firstSplit := strings.SplitN(value, "_", 2)
		if len(firstSplit) < 2 {
			// panic("NOT VALID RECORD " + value)
			emitter.Emit(key, "NOT GOOD ENOUGH: "+value)
			return
		}
		opening := firstSplit[0]
		r := csv.NewReader(strings.NewReader(firstSplit[1]))
		r.LazyQuotes = true
		line, _ := r.Read()
		switch opening {
		case "MOVIE": // Movie Info Record
			if len(line) < 20 {
				panic("NOT 20 records in " + value)
			}
			x.id = line[5]
			x.title = line[20]
			emitter.Emit(key, value)
			return
		case "CREDITS":
			x.firstActor = line[0]
		default:
			emitter.Emit(key, "OPENING NOT GOOD ENOUGH: "+opening)
			return
		}
	}
	output, _ := json.Marshal(x)
	emitter.Emit(key, string(output))
}

func main() {
	job := corral.NewJob(wordCount{}, wordCount{})

	options := []corral.Option{
		corral.WithSplitSize(10 * 1024),
		corral.WithMapBinSize(10 * 1024),
	}

	driver := corral.NewDriver(job, options...)
	driver.Main()
}
