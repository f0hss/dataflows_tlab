package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

type rowTweet struct {
	tweetId                    string
	textContent                string
	mediaContent               string
	tweetDate                  string
	nbLikes                    string
	nbRetweets                 string
	nbComments                 string
	userId                     string
	subjectId                  string
	sentimentAnalysisScore     string
	sentimentAnalysisMagnitude string
}

func getBqSchema(filepath string) bigquery.Schema {
	dat, e := ioutil.ReadFile(filepath)
	if e != nil {
		log.Fatal("Json schema not found")
	}
	schema, e := bigquery.SchemaFromJSON(dat)
	if e != nil {
		log.Fatal("Parsing failed")
	}
	return schema
}

func mapLines(line string) rowTweet {
	splittedLine := strings.Split(line, "+_/")

	row := rowTweet{
		tweetId:                    splittedLine[0],
		textContent:                splittedLine[1],
		mediaContent:               splittedLine[2],
		tweetDate:                  splittedLine[3],
		nbLikes:                    splittedLine[4],
		nbRetweets:                 splittedLine[5],
		nbComments:                 splittedLine[6],
		userId:                     splittedLine[7],
		subjectId:                  splittedLine[8],
		sentimentAnalysisScore:     "TODO",
		sentimentAnalysisMagnitude: "TODO",
	}
	fmt.Printf("%+v\n", row)
	return row
}

func main() {
	var (
		tableID   = flag.String("tableID", "", "Table name in GCP")
		inputFile = flag.String("inputFile", "", "Location to file")
	)
	flag.Parse()
	beam.Init()
	// Beginning Pipeline
	p, s := beam.NewPipelineWithRoot()

	// I don't know yet what I'm doing
	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	// Parsing flags to get a qualified name
	// schema := getBqSchema("tweets_raw.json")

	// Reading file
	rawLines := textio.Read(s, *inputFile)
	mappedLines := beam.ParDo(s, mapLines, rawLines)
	bigqueryio.Write(s, project, *tableID, mappedLines)

	fmt.Printf(mappedLines.String())
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
