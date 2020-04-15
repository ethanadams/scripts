package main

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"

	"github.com/zeebo/errs"
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
	log.Printf("Listing objects\n")
	iter := project.ListObjects(ctx, bucket.Name, &listOptions)
	log.Printf("Received list\n")
	keys := []string{}
	for iter.Next() {
		item := iter.Item()

		if item.IsPrefix {
			log.Printf("Skipping prefix %v\n", item.Key)
			continue
		}

		//log.Printf("Adding key %v\n", item.Key)
		keys = append(keys, item.Key)
	}
	if iter.Err() != nil {
		log.Fatalf("Iteration error %v\n", iter.Err())
		return
	}
	log.Printf("Iteration complete\n")

	group := new(errs2.Group)
	for i := 0; i < *workersArg; i++ {
		worker := i
		group.Go(func() error {
			err := run(ctx, worker, project, bucket, keys)
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

func run(ctx context.Context, worker int, project *uplink.Project, bucket *uplink.Bucket, keys []string) error {
	log.Printf("[%v] Running\n", worker)
	var read int64
	for i, k := range keys {
		reader, err := project.DownloadObject(ctx, bucket.Name, k, nil)
		if err != nil {
			return err
		}

		//log.Printf("[%v] Downloading %v%v\n", worker, bucket.Name, k)
		var r int64
		if r, err = io.Copy(ioutil.Discard, reader); err != nil {
			log.Fatalf("%v\n", err)

			return errs.Combine(err, reader.Close())
		}
		err = reader.Close()
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		read += r
		if i%10 == 0 {
			log.Printf("[%v] Downloaded %v bytes", worker, read)
		}
	}
	return nil
}
