package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"storj.io/common/sync2"
	"storj.io/uplink"
)

func main() {
	ctx := context.Background()

	accessArg := flag.String("a", "", "Access token")
	bucketArg := flag.String("b", "", "Bucket name")
	pathArg := flag.String("p", "", "Folder path")
	workersArg := flag.Int("w", 1, "Number of workers")
	flag.Parse()

	fmt.Printf("WORKERS %v\n", *workersArg)
	access, err := uplink.ParseAccess(*accessArg)
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

	bucket, err := project.EnsureBucket(ctx, *bucketArg)
	if err != nil {
		log.Fatalf("%v\n", err)
		return
	}

	listOptions := uplink.ListObjectsOptions{
		Prefix:    *pathArg,
		Recursive: true,
	}
	iter := project.ListObjects(ctx, bucket.Name, &listOptions)

	keys := []string{}
	for iter.Next() {
		item := iter.Item()
		keys = append(keys, item.Key)
	}

	limiter := sync2.NewLimiter(*workersArg)
	go func() {
		for i := 0; i < *workersArg; i++ {
			limiter.Go(ctx, func() {
				err := run(ctx, project, bucket, keys)
				if err != nil {
					log.Fatalf("%v\n", err)
				}
			})
		}
	}()
	log.Println("Waiting...")
	limiter.Wait()
	log.Println("Done!")
}

func run(ctx context.Context, project *uplink.Project, bucket *uplink.Bucket, keys []string) error {
	log.Printf("Running %v\n", bucket.Name)
	for _, k := range keys {
		log.Printf("Downloading %v%v\n", bucket.Name, k)
		reader, err := project.DownloadObject(ctx, bucket.Name, k, nil)
		if err != nil {
			return err
		}

		defer reader.Close()

		if _, err := io.Copy(ioutil.Discard, reader); err != nil {
			log.Fatalf("%v\n", err)
			return err
		}
	}
	return nil
}
