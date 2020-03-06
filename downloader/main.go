package main

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"

	"storj.io/common/errs2"
	"storj.io/uplink"
)

func main() {
	ctx := context.Background()

	accessArg := flag.String("a", "", "Access token")
	bucketArg := flag.String("b", "", "Bucket name")
	pathArg := flag.String("p", "", "Folder path")
	workersArg := flag.Int("w", 1, "Number of workers")
	flag.Parse()
	/*
		log.Printf("-a %v, -b %v, -p %v, -w %v\n",
			*accessArg,
			*bucketArg,
			*pathArg,
			*workersArg)
	*/
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

	group := new(errs2.Group)
	for i := 0; i < *workersArg; i++ {
		group.Go(func() error {
			err := run(ctx, project, bucket, keys)
			if err != nil {
				log.Fatalf("%v\n", err)
				return err
			}
			return nil
		})
	}
	log.Println("Waiting...")
	errs := group.Wait()
	if errs != nil {
		for _, err := range errs {
			log.Printf("%v", err)
		}
	}
	log.Println("Done!")
}

func run(ctx context.Context, project *uplink.Project, bucket *uplink.Bucket, keys []string) error {
	log.Printf("Running\n")
	for _, k := range keys {
		reader, err := project.DownloadObject(ctx, bucket.Name, k, nil)
		if err != nil {
			return err
		}

		defer reader.Close()
		log.Printf("Downloading %v%v\n", bucket.Name, k)
		if _, err := io.Copy(ioutil.Discard, reader); err != nil {
			log.Fatalf("%v\n", err)
			return err
		}
	}
	return nil
}
