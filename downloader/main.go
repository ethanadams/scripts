package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"storj.io/common/sync2"
	"storj.io/uplink"
)

func main() {
	ctx := context.Background()

	accessArg := os.Getenv("PROJECT_ACCESS")
	bucketArg := os.Getenv("BUCKET_NAME")
	pathArg := os.Getenv("DOWLOAD_PATH")

	workers := flag.Int("w", 1, "Number of workers")
	flag.Parse()

	fmt.Printf("WORKERS %v\n", *workers)
	access, err := uplink.ParseAccess(accessArg)
	if err != nil {
		log.Fatalf("%v\n", err)
		return
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		log.Fatalf("%v\n", err)
		return
	}
	defer project.Close()

	bucket, err := project.EnsureBucket(ctx, bucketArg)
	if err != nil {
		log.Fatalf("%v\n", err)
		return
	}

	listOptions := uplink.ListObjectsOptions{
		Prefix:    pathArg,
		Recursive: true,
	}
	iter := project.ListObjects(ctx, bucket.Name, &listOptions)

	keys := []string{}
	for iter.Next() {
		item := iter.Item()
		keys = append(keys, item.Key)
	}

	limiter := sync2.NewLimiter(*workers)

	for i := 0; i < *workers; i++ {
		limiter.Go(ctx, func() {
			err := run(ctx, project, bucket, keys)
			if err != nil {
				log.Fatalf("%v\n", err)
			}
		})
	}
	limiter.Wait()
}

func run(ctx context.Context, project *uplink.Project, bucket *uplink.Bucket, keys []string) error {
	log.Printf("Running %v\n", bucket.Name)
	for _, k := range keys {
		log.Printf("Downloading %v%v\n", bucket.Name, k)
		download, err := project.DownloadObject(ctx, bucket.Name, k, nil)
		if err != nil {
			return err
		}

		defer download.Close()

		if _, err := ioutil.ReadAll(download); err != nil {
			log.Fatalf("%v\n", err)
			return err
		}
	}
	return nil
}
