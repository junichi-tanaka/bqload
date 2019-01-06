package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
)

func importJSONTruncate(client *bigquery.Client, datasetID, tableID, source string) error {
	ctx := context.Background()

	gcsRef := bigquery.NewGCSReference(source)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	return nil
}

func main() {
	projectID := flag.String("project", "", "gcp project id")
	datasetID := flag.String("dataset", "", "dataset id")
	tableID := flag.String("table", "", "table id")
	source := flag.String("source", "", "source path on GCS")
	flag.Parse()

	if *projectID == "" {
		log.Fatal("project id must be set")
	}
	if *datasetID == "" {
		log.Fatal("dataset id must be set")
	}
	if *tableID == "" {
		log.Fatal("table id must be set")
	}
	if *source == "" {
		log.Fatal("source must be set")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatal(err)
	}

	err = importJSONTruncate(client, *datasetID, *tableID, *source)
	if err != nil {
		log.Fatal(err)
	}
}
