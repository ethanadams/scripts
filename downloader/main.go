package main

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	monkit "github.com/spacemonkeygo/monkit/v3"
	"github.com/spacemonkeygo/monkit/v3/environment"
	"github.com/spacemonkeygo/monkit/v3/present"
	"github.com/zeebo/admission/v3/admproto"
	"go.uber.org/zap"
	"storj.io/common/errs2"
	"storj.io/common/memory"
	"storj.io/common/telemetry"
	"storj.io/private/version"
	"storj.io/uplink"
)

var (
	mon = monkit.Package()
)

func main() {
	ctx := context.Background()

	accessArg := flag.String("a", "", "Access token")
	bucketArg := flag.String("b", "", "Bucket name")
	pathArg := flag.String("p", "", "Folder path")
	workersArg := flag.Int("w", 1, "Number of workers")
	forever := flag.Bool("f", false, "Loop forever")
	metrics := flag.Bool("m", false, "Send metrics")
	delete := flag.Bool("delete", false, "Send metrics")

	action := "download"
	if *delete {
		action = "delete"
	}

	flag.Parse()
	/*
		log.Printf("-a %+v, -b %+v, -p %+v, -w %+v\n",
			*accessArg,
			*bucketArg,
			*pathArg,
			*workersArg)
	*/
	go http.ListenAndServe(":9000", present.HTTP(monkit.Default))
	if *metrics {
		_, err := initMetrics(ctx)
		if err != nil {
			log.Fatalf("%+v\n", err)
			return
		}
		log.Println("Metrics sender started")
	}

	access, err := uplink.ParseAccess(*accessArg)
	if err != nil {
		log.Fatalf("%+v\n", err)
		return
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		log.Fatalf("%+v\n", err)
		return
	}
	defer project.Close()

	bucket, path, keys, err := getKeys(ctx, project, *bucketArg, *pathArg)
	if err != nil {
		log.Fatalf("%+v\n", err)
		return
	}

	log.Printf("Starting %ss: bucket=%+v, path=%+v\n", action, bucket.Name, path)

	for {
		group := new(errs2.Group)
		for i := 0; i < *workersArg; i++ {
			worker := i
			group.Go(func() error {
				err := run(ctx, worker, project, bucket, keys, *delete)
				if err != nil {
					log.Fatalf("%+v\n", err)
					return err
				}
				return nil
			})
		}
		log.Println("Waiting...")
		errs := group.Wait()
		if errs != nil {
			for _, err := range errs {
				log.Printf("%+v", err)
			}
		}
		if !*forever {
			break
		}
	}
	log.Println("Done!")
}

func getKeys(ctx context.Context, project *uplink.Project, bucketArg string, pathArg string) (bucket *uplink.Bucket, path string, keys []string, err error) {
	var bucketName string
	path = ""

	if bucketArg == "" {
		// Find one
		bucketIter := project.ListBuckets(ctx, nil)
		var buckets []string
		for bucketIter.Next() {
			item := bucketIter.Item()

			buckets = append(buckets, item.Name)
		}
		if bucketIter.Err() != nil {
			log.Fatalf("Iteration error %+v\n", bucketIter.Err())
			return bucket, path, keys, bucketIter.Err()
		}
		rs := rand.NewSource(time.Now().UnixNano())
		r := rand.New(rs)
		random := r.Intn(len(buckets))

		bucketName = buckets[random]
	} else {
		bucketName = bucketArg
		if pathArg == "" {
			path = pathArg
		}
	}

	bucket, err = project.EnsureBucket(ctx, bucketName)
	if err != nil {
		log.Fatalf("%+v\n", err)
		return bucket, path, keys, err
	}

	listOptions := uplink.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	}
	log.Printf("Listing objects\n")
	iter := project.ListObjects(ctx, bucket.Name, &listOptions)
	log.Printf("Received list\n")
	//keys := []string{}
	for iter.Next() {
		item := iter.Item()

		if item.IsPrefix {
			log.Printf("Skipping prefix %+v\n", item.Key)
			continue
		}

		//log.Printf("Adding key %+v\n", item.Key)
		keys = append(keys, item.Key)
	}
	if iter.Err() != nil {
		log.Fatalf("Iteration error %+v\n", iter.Err())
		return bucket, path, keys, iter.Err()
	}
	log.Printf("Iteration complete\n")

	return bucket, path, keys, nil
}

func run(ctx context.Context, worker int, project *uplink.Project, bucket *uplink.Bucket, keys []string, delete bool) error {
	log.Printf("[%+v] Running\n", worker)
	var read int64
	var deleted int64
	for i, k := range keys {
		if delete {
			o, err := project.DeleteObject(ctx, bucket.Name, k)
			if err != nil {
				//log.Printf("%+v\n", err)
				continue
			}
			read += o.System.ContentLength
			deleted++
			mon.IntVal("bytes_deleted").Observe(deleted)
			if i%10 == 0 {
				log.Printf("[%+v] Deleted %v - total bytes %v", worker, deleted, read)
			}
			continue
		}

		reader, err := project.DownloadObject(ctx, bucket.Name, k, nil)
		if err != nil {
			return err
		}

		//log.Printf("[%+v] Downloading %+v%+v\n", worker, bucket.Name, k)
		var r int64
		if r, err = io.Copy(ioutil.Discard, reader); err != nil {
			log.Printf("%+v\n", err)

			//return errs.Combine(err, reader.Close())
		}

		err = reader.Close()
		if err != nil {
			log.Printf("%+v\n", err)
		}

		read += r
		mon.IntVal("bytes_downloaded").Observe(read)
		if i%10 == 0 {
			log.Printf("[%+v] Downloaded %v", worker, memory.Size(read))
		}
	}
	return nil
}

func initMetrics(ctx context.Context) (r *monkit.Registry, err error) {
	r = monkit.Default

	environment.Register(r)
	r.ScopeNamed("env").Chain(monkit.StatSourceFunc(version.Build.Stats))

	log, _ := zap.NewDevelopment()
	var metricsID string
	hostname, err := os.Hostname()
	if err != nil {
		return r, nil
	}
	metricsID = strings.ReplaceAll(hostname, ".", "_")

	c, err := telemetry.NewClient(log,
		"collectora.storj.io:9000",
		telemetry.ClientOpts{
			Application:   "scale-n-effic-downloader",
			Instance:      metricsID,
			Registry:      r,
			FloatEncoding: admproto.Float32Encoding,
		})
	if err != nil {
		return r, err
	}
	go c.Run(ctx)

	return r, nil
}
