//
// Copyright (c) 2017 Pantacor Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

//
// mgo2elastic provides a CLI experience for mass importing mongo collections
// into elasticsearch indexes.
//
// See: ```mgo2elastic --help for details````
//
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"os"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/Masterminds/sprig"
	"github.com/olivere/elastic"
	"github.com/urfave/cli"
)

var (
	failed bool
)

// The ImportJob parameters in a typed go format
type ImportJob struct {
	// Template data passed to all go template parameters.
	// The data will be passed to the template using the "data" key.
	TemplateData interface{}

	// Funcs holds the funcmap available to templates.
	Funcs template.FuncMap

	// From URL as a go template string used to connect to mongodb
	From     string
	fromTmpl *template.Template

	// Collection as a go template to export from mongodb db
	Collection     string
	collectionTmpl *template.Template

	// To URL as a go template to connect to elasticsearch.
	To     string
	toTmpl *template.Template

	// Type as a go template string; defines the elastic type for this doc.
	// The mongo doc will be passed to the template using the "doc" key for
	// every document processed.
	Type     string
	typeTmpl *template.Template

	// Index as a go template string; defines the elastic index for this doc
	// On top of `TemplateData` above, the mongo doc to import will be passed
	// to the template using the "doc" key for every document processed.
	Index     string
	indexTmpl *template.Template

	// ElasticWorkers sets the number of worker routine for parallel uploads
	// to elastic.
	ThreadCount int

	ms *mgo.Session
}

func DefaultFuncMap() template.FuncMap {
	funcMap := sprig.FuncMap()
	return funcMap
}

func ParseInput(job *ImportJob) error {
	var err error

	errBuf := bytes.NewBufferString("")
	if job.From == "" {
		errBuf.WriteString(fmt.Sprintf("ERROR: missing parameter 'from'. See usage. Thx!\n"))
	}
	if job.To == "" {
		errBuf.WriteString(fmt.Sprintf("ERROR: missing parameter 'from'. See usage. Thx!\n"))
	}
	if job.Index == "" {
		errBuf.WriteString(fmt.Sprintf("ERROR: missing parameter 'index'. See usage. Thx!\n"))
	}
	if job.Collection == "" {
		errBuf.WriteString(fmt.Sprintf("ERROR: missing parameter 'collection'. See usage. Thx!\n"))
	}

	job.fromTmpl = template.New("from")
	job.fromTmpl = job.fromTmpl.Funcs(job.Funcs)
	job.fromTmpl, err = job.fromTmpl.Parse(job.From)
	if err != nil {
		errBuf.WriteString(fmt.Sprintf("ERROR: bad template for parameter 'from' %s\n", err.Error()))
	}

	job.collectionTmpl = template.New("collection")
	job.collectionTmpl = job.collectionTmpl.Funcs(job.Funcs)
	job.collectionTmpl, err = job.collectionTmpl.Parse(job.Collection)
	if err != nil {
		errBuf.WriteString(fmt.Sprintf("ERROR: bad template for parameter 'collection' %s\n", err.Error()))
	}

	job.toTmpl = template.New("to")
	job.toTmpl = job.toTmpl.Funcs(job.Funcs)
	job.toTmpl, err = job.toTmpl.Parse(job.To)
	if err != nil {
		errBuf.WriteString(fmt.Sprintf("ERROR: bad template for parameter 'to' %s\n", err.Error()))
	}

	job.indexTmpl = template.New("index")
	job.indexTmpl = job.indexTmpl.Funcs(job.Funcs)
	job.indexTmpl, err = job.indexTmpl.Parse(job.Index)
	if err != nil {
		errBuf.WriteString(fmt.Sprintf("ERROR: bad template for parameter 'index' %s\n", err.Error()))
	}
	job.typeTmpl = template.New("type")
	job.typeTmpl = job.typeTmpl.Funcs(job.Funcs)
	// empty type -> use collection template
	if job.Type == "" {
		job.typeTmpl, err = job.typeTmpl.Parse(job.Collection)
	} else {
		job.typeTmpl, err = job.typeTmpl.Parse(job.Type)
	}
	if err != nil {
		errBuf.WriteString(fmt.Sprintf("ERROR: bad template for parameter 'index' %s\n", err.Error()))
	}

	if errBuf.Len() > 0 {
		return errors.New(errBuf.String())
	}
	return nil
}

func evalTmpl(tmpl *template.Template, job *ImportJob, doc interface{}) (*bytes.Buffer, error) {
	tmplData := map[string]interface{}{}

	tmplData["data"] = job.TemplateData
	tmplData["doc"] = doc

	resBuf := bytes.NewBufferString("")
	err := tmpl.Execute(resBuf, tmplData)
	if err != nil {
		return nil, err
	}
	return resBuf, nil
}

func dialDB(job *ImportJob) error {

	buf, err := evalTmpl(job.fromTmpl, job, nil)
	if err != nil {
		return err
	}

	log.Printf("- Importing from mgo URL: %s\n", buf.String())

	job.ms, err = mgo.Dial(buf.String())
	if err != nil {
		return err
	}
	return nil
}

type ImportEventType int

const (
	BulkDone     = ImportEventType(1)
	DoError      = ImportEventType(2)
	ChannelError = ImportEventType(3)
	ProcessError = ImportEventType(4)
	AllDone      = ImportEventType(5)
)

type ImportEvent struct {
	Type    ImportEventType
	Summary string
	Subject interface{}
}

func allDoneEvent(subject interface{}, eventChan chan *ImportEvent) {
	e := &ImportEvent{
		Type:    AllDone,
		Summary: "All Done.",
		Subject: subject,
	}
	eventChan <- e
}

func bulkDoneEvent(response *elastic.BulkResponse, subject interface{}, eventChan chan *ImportEvent) {
	e := &ImportEvent{
		Type:    BulkDone,
		Summary: fmt.Sprintf("Inserted bulk docs (count=%d)\n", len(response.Items)),
		Subject: response,
	}
	eventChan <- e
}

func errorEvent(err error, event ImportEventType, subject interface{}, eventChan chan *ImportEvent) {
	e := &ImportEvent{
		Type:    event,
		Summary: err.Error(),
		Subject: subject,
	}
	eventChan <- e
}

func worker(job *ImportJob, docChan chan map[string]interface{}, eventChan chan *ImportEvent) error {

	var bulkCount int

	toUrlBuf, err := evalTmpl(job.toTmpl, job, nil)

	if err != nil {
		return err
	}

	log.Printf("- Importing to elastic URL with worker: %s\n", toUrlBuf.String())

	client, err := elastic.NewClient(elastic.SetURL(toUrlBuf.String()))

	if err != nil {
		return err
	}

	bulk := client.Bulk()

	for doc := range docChan {
		idVal, ok := doc["_id"]
		if ok {
			doc["mongoid"] = idVal
			delete(doc, "_id")
		}
		// first process index and docs
		indexStr, err := evalTmpl(job.indexTmpl, job, doc)
		if err != nil {
			errorEvent(err, ProcessError, doc, eventChan)
			continue
		}
		typeStr, err := evalTmpl(job.typeTmpl, job, doc)
		if err != nil {
			errorEvent(err, ProcessError, doc, eventChan)
			continue
		}

		// transform _id to elastic id
		bulkReq := elastic.NewBulkIndexRequest()
		bulkReq = bulkReq.Index(indexStr.String()).Type(typeStr.String()).Doc(doc)
		bulk.Add(bulkReq)
		bulkCount++
		// lets push 64K chunks for now
		if bulk.EstimatedSizeInBytes() > 1024*512 {
			ctx, cancelFunc := context.WithTimeout(context.Background(),
				time.Duration(60)*time.Second)

			response, err := bulk.Do(ctx)
			if err != nil {
				cancelFunc()
				errorEvent(err, DoError, doc, eventChan)
				continue
			}
			bulkDoneEvent(response, job, eventChan)
		}
	}
	allDoneEvent(job, eventChan)

	return err
}

func DoImport(job *ImportJob) error {

	var doc map[string]interface{}
	var i *mgo.Iter
	err := dialDB(job)

	if err != nil {
		return err
	}

	docChan := make(chan map[string]interface{}, 100)
	eventChan := make(chan *ImportEvent, 100)

	// spin up workers
	for i := 0; i < job.ThreadCount; i++ {
		go worker(job, docChan, eventChan)
	}

	collectionBuf, err := evalTmpl(job.collectionTmpl, job, nil)

	if err != nil {
		return err
	}

	log.Printf("- Importing from collection: %s\n", collectionBuf.String())

	i = job.ms.DB("").C(collectionBuf.String()).Find(bson.M{}).Iter()

	if !i.Next(&doc) {
		err := errors.New("no docs could be retried using current configuration; see --help")
		close(docChan)
		errorEvent(err, ChannelError, job, eventChan)
		return err
	}

	allDoneCount := 0
	jobChanDoneCount := 0

	docRead := 0

	for allDoneCount < job.ThreadCount || jobChanDoneCount < 1 {
		select {
		case docChan <- doc:
			doc = map[string]interface{}{}
			nextDoc := i.Next(&doc)
			if !nextDoc {
				close(docChan)
				docChan = nil
				jobChanDoneCount++
			}
			docRead++
			break
		case ev := <-eventChan:
			if ev.Type == AllDone {
				allDoneCount++
			} else if ev.Type == ChannelError {
				allDoneCount++
			} else if ev.Type == ProcessError {
				allDoneCount++
			}
			break
		}
	}

	return nil
}

func main() {

	app := cli.NewApp()
	app.Name = "mgo2elastic"
	app.Usage = "import mongo collections into elastic"
	app.ArgsUsage = "mgo2elastic [-f|--from <MONGO_URL>] -t|--to <ELASTIC_URL> -i|--index <ELASTIC_INDEX>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "f,from",
			Value:  "mongodb://localhost",
			Usage:  "The `MONGO_URL` pointing to the db to export for import into elastic",
			EnvVar: "M2E_MONGO_URL",
		},
		cli.StringFlag{
			Name:   "c,collection",
			Value:  "default",
			Usage:  "The `COLLECTION` pointing to the collection to export for import into elastic",
			EnvVar: "M2E_COLLECTION",
		},
		cli.StringFlag{
			Name:   "t,to",
			Value:  "http://localhost:9200",
			Usage:  "The `ELASTIC_URL` to import to",
			EnvVar: "M2E_ELASTIC_URL",
		},
		cli.StringFlag{
			Name:   "i,index",
			Value:  "mgo2elastic-{{ now | date \"2006-01-02\" }}",
			Usage:  "The `ELASTIC_INDEX` to import into to. Is a golang template with sprig template func (-> https://github.com/Masterminds/sprig)",
			EnvVar: "M2E_ELASTIC_INDEX",
		},
		cli.IntFlag{
			Name:   "threads",
			Value:  2,
			Usage:  "The amount of `THREADS` (aka goroutine) works to spawn for parallel import into elastic",
			EnvVar: "M2E_THREADS",
		},
		cli.StringFlag{
			Name:   "type",
			Value:  "",
			Usage:  "The `ELASTIC_TYPE` used for indexing the documents. default is collection name",
			EnvVar: "M2E_ELASTIC_TYPE",
		},
	}
	app.Action = func(c *cli.Context) error {

		job := &ImportJob{
			Funcs:       DefaultFuncMap(),
			From:        c.String("from"),
			Collection:  c.String("collection"),
			To:          c.String("to"),
			Index:       c.String("index"),
			Type:        c.String("type"),
			ThreadCount: c.Int("threads"),
		}

		err := ParseInput(job)

		if err != nil {
			fmt.Println(err.Error())
			cli.ShowAppHelpAndExit(c, 1)
		}

		err = DoImport(job)

		if err != nil {
			return cli.NewExitError("import failed with error: "+err.Error(), 100)
		}

		log.Println("Success.")
		return nil
	}

	app.Run(os.Args)
}
