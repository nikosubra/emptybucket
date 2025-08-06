package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/schollz/progressbar/v3"
)

func main() {
	dryRun := false // Set to true for dry run mode

	fmt.Println("Starting bucket cleanup script...")
	start := time.Now()

	// Prompt user for AWS credentials and bucket info
	var accessKey, secretKey, bucket, endpoint, region string
	fmt.Print("Enter AWS Access Key: ")
	fmt.Scanln(&accessKey)
	fmt.Print("Enter AWS Secret Key: ")
	fmt.Scanln(&secretKey)
	fmt.Print("Enter Bucket name: ")
	fmt.Scanln(&bucket)
	fmt.Print("Enter S3 Endpoint: ")
	fmt.Scanln(&endpoint)
	fmt.Print("Enter Region: ")
	fmt.Scanln(&region)

	// LOG FILES
	f, err := os.Create("output.log")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	log.SetOutput(f)
	jsonLogFile, err := os.Create("log_json.json")
	if err != nil {
		log.Printf("Error opening log_json file: %v\n", err)
	}
	defer jsonLogFile.Close()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			aws.NewCredentialsCache(
				aws.StaticCredentialsProvider{
					Value: aws.Credentials{
						AccessKeyID:     accessKey,
						SecretAccessKey: secretKey,
						SessionToken:    "",
						Source:          "user-input",
					},
				},
			),
		),
		config.WithEndpointResolver(
			aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}),
		),
	)
	if err != nil {
		log.Printf("Config error: %v\n", err)
		os.Exit(1)
	}

	client := s3.NewFromConfig(cfg)

	// Check basic permissions with a head bucket and versioning info
	headCtx, headCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer headCancel()
	_, err = client.HeadBucket(headCtx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatalf("Insufficient permissions or bucket not accessible: %v", err)
	}
	// Get versioning configuration
	verCfg, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Printf("Error retrieving versioning configuration: %v", err)
	} else {
		log.Printf("Versioning: %s | MFA Delete: %s", string(verCfg.Status), string(verCfg.MFADelete))
	}
	// Save bucket config in json log
	json.NewEncoder(jsonLogFile).Encode(map[string]interface{}{
		"time":             time.Now().Format(time.RFC3339),
		"bucket":           bucket,
		"region":           region,
		"endpoint":         endpoint,
		"versioningStatus": string(verCfg.Status),
		"mfaDelete":        string(verCfg.MFADelete),
	})

	// Calculate total objects (Versions + DeleteMarkers)
	log.Println("Starting total object count in bucket...")
	var mu sync.Mutex
	var totalObjects int
	countPaginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	for countPaginator.HasMorePages() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		page, err := countPaginator.NextPage(ctx)
		cancel()
		if err != nil {
			log.Printf("Error during object counting on current page: %v", err)
			log.Fatalf("Counting interrupted: %v", err)
		}
		mu.Lock()
		totalObjects += len(page.Versions) + len(page.DeleteMarkers)
		mu.Unlock()
	}
	fmt.Printf("Total objects detected in the bucket: %d\n", totalObjects)
	log.Printf("Total objects detected in the bucket: %d\n", totalObjects)

	// Progress bar
	bar := progressbar.NewOptions(totalObjects,
		progressbar.OptionSetDescription("Deleting objects"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(30),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)

	// Snapshot and state
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	snapshotFile := "snapshot_progress.log"
	stateFile := "state.json"
	var deletedCount, errorCount int
	type State struct {
		DeletedCount int `json:"deletedCount"`
		ErrorCount   int `json:"errorCount"`
	}
	saveState := func() {
		state := State{DeletedCount: deletedCount, ErrorCount: errorCount}
		b, _ := json.MarshalIndent(state, "", "  ")
		_ = os.WriteFile(stateFile, b, 0644)
	}
	go func() {
		for range ticker.C {
			mu.Lock()
			elapsed := time.Since(start)
			var eta time.Duration
			if deletedCount > 0 && totalObjects > deletedCount {
				rate := float64(elapsed.Milliseconds()) / float64(deletedCount)
				eta = time.Duration(rate*float64(totalObjects-deletedCount)) * time.Millisecond
			}
			var percent float64
			if totalObjects > 0 {
				percent = (float64(deletedCount) / float64(totalObjects)) * 100
			}
			log.Printf("[STATUS] Deleted: %d | Errors: %d | ETA: %v | Completed: %.2f%%\n", deletedCount, errorCount, eta, percent)
			snapshotContent := map[string]interface{}{
				"time":    time.Now().Format(time.RFC3339),
				"deleted": deletedCount,
				"errors":  errorCount,
				"eta":     eta.String(),
				"percent": percent,
			}
			b, _ := json.MarshalIndent(snapshotContent, "", "  ")
			_ = os.WriteFile(snapshotFile, b, 0644)
			saveState()
			mu.Unlock()
		}
	}()

	// Dry run CSV
	var dryRunCSV *os.File
	var dryRunWriter *csv.Writer
	if dryRun {
		dryRunCSV, err = os.Create("dryrun_export.csv")
		if err != nil {
			log.Printf("Error creating dryrun_export.csv: %v", err)
		} else {
			dryRunWriter = csv.NewWriter(dryRunCSV)
			dryRunWriter.Write([]string{"Key", "VersionId"})
		}
	}

	// For retry and failure tracking
	type FailedObject struct {
		Key       string
		VersionId string
		Code      string
		Message   string
	}
	var failedObjects []FailedObject
	var failedObjectsMu sync.Mutex

	// Pagination: list object versions
	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})

	const batchSize = 500
	numWorkers := runtime.NumCPU() * 2
	sem := make(chan struct{}, numWorkers)
	var wg sync.WaitGroup

	for paginator.HasMorePages() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		page, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			log.Printf("Error listing versions: %v\n", err)
			os.Exit(1)
		}

		log.Printf("Loaded page with %d Versions and %d DeleteMarkers\n", len(page.Versions), len(page.DeleteMarkers))

		var currentBatch []types.ObjectIdentifier

		processBatch := func() {
			if len(currentBatch) == 0 {
				return
			}
			wg.Add(1)
			sem <- struct{}{}
			seen := make(map[string]struct{})
			deduplicated := make([]types.ObjectIdentifier, 0, len(currentBatch))
			for _, obj := range currentBatch {
				key := fmt.Sprintf("%s:%s", aws.ToString(obj.Key), aws.ToString(obj.VersionId))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				deduplicated = append(deduplicated, obj)
			}
			batchCopy := make([]types.ObjectIdentifier, len(deduplicated))
			copy(batchCopy, deduplicated)
			go func(toDelete []types.ObjectIdentifier) {
				defer wg.Done()
				log.Printf("Starting deletion of batch with %d objects", len(toDelete))
				if dryRun {
					log.Printf("[DRY-RUN] Simulating deletion of %d objects\n", len(toDelete))
					if dryRunWriter != nil {
						for _, obj := range toDelete {
							dryRunWriter.Write([]string{aws.ToString(obj.Key), aws.ToString(obj.VersionId)})
						}
						dryRunWriter.Flush()
					}
					<-sem
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				input := &s3.DeleteObjectsInput{
					Bucket:                    aws.String(bucket),
					Delete:                    &types.Delete{Objects: toDelete},
					BypassGovernanceRetention: aws.Bool(true),
				}
				var resp *s3.DeleteObjectsOutput
				var err error
				for i := 0; i < 3; i++ {
					resp, err = client.DeleteObjects(ctx, input)
					if err == nil {
						break
					}
					log.Printf("Attempt %d failed: %v\n", i+1, err)
					time.Sleep(time.Duration(i+1) * time.Second)
				}
				mu.Lock()
				if err != nil {
					errorCount += len(toDelete)
					log.Printf("Delete batch error after 3 attempts: %v\n", err)
					for _, obj := range toDelete {
						failedObjectsMu.Lock()
						failedObjects = append(failedObjects, FailedObject{
							Key:       aws.ToString(obj.Key),
							VersionId: aws.ToString(obj.VersionId),
							Code:      "Unknown",
							Message:   err.Error(),
						})
						failedObjectsMu.Unlock()
					}
					mu.Unlock()
					<-sem
					return
				}
				deletedCount += len(resp.Deleted)
				errorCount += len(resp.Errors)
				_ = bar.Add(len(resp.Deleted))
				log.Printf("Deleted %d objects (with %d errors)\n", len(resp.Deleted), len(resp.Errors))
				for _, e := range resp.Errors {
					log.Printf("Error on object: Key=%s, VersionId=%s, Code=%s, Message=%s",
						aws.ToString(e.Key), aws.ToString(e.VersionId), aws.ToString(e.Code), aws.ToString(e.Message))
					failedObjectsMu.Lock()
					failedObjects = append(failedObjects, FailedObject{
						Key:       aws.ToString(e.Key),
						VersionId: aws.ToString(e.VersionId),
						Code:      aws.ToString(e.Code),
						Message:   aws.ToString(e.Message),
					})
					failedObjectsMu.Unlock()
				}
				serviceUnavailableCount := 0
				for _, e := range resp.Errors {
					if aws.ToString(e.Code) == "ServiceUnavailable" {
						serviceUnavailableCount++
					}
				}
				if serviceUnavailableCount >= 3 {
					pause := time.Duration(serviceUnavailableCount) * time.Second
					log.Printf("Detected %d ServiceUnavailable errors. Pausing for %v for backoff.\n", serviceUnavailableCount, pause)
					time.Sleep(pause)
				}
				mu.Unlock()
				<-sem
			}(batchCopy)
			currentBatch = nil
		}

		for _, v := range page.Versions {
			currentBatch = append(currentBatch, types.ObjectIdentifier{
				Key:       v.Key,
				VersionId: v.VersionId,
			})
			if len(currentBatch) == batchSize {
				processBatch()
			}
		}
		for _, dm := range page.DeleteMarkers {
			currentBatch = append(currentBatch, types.ObjectIdentifier{
				Key:       dm.Key,
				VersionId: dm.VersionId,
			})
			if len(currentBatch) == batchSize {
				processBatch()
			}
		}
		processBatch()
	}

	if dryRun && dryRunWriter != nil {
		dryRunWriter.Flush()
		dryRunCSV.Close()
		fmt.Println("Dry-run export completed in dryrun_export.csv")
		return
	}

	fmt.Println("Waiting for workers to finish...")
	log.Println("Waiting for workers to finish...")
	wg.Wait()

	// Smart retry: second pass for failed objects
	if len(failedObjects) > 0 {
		log.Printf("Starting second attempt for %d failed objects...", len(failedObjects))
		fmt.Printf("Starting second attempt for %d failed objects...\n", len(failedObjects))
		var retryWg sync.WaitGroup
		var retrySem = make(chan struct{}, numWorkers)
		var stillFailed []FailedObject
		for i := 0; i < len(failedObjects); i += batchSize {
			end := i + batchSize
			if end > len(failedObjects) {
				end = len(failedObjects)
			}
			batch := failedObjects[i:end]
			retryWg.Add(1)
			retrySem <- struct{}{}
			go func(batch []FailedObject) {
				defer retryWg.Done()
				var objs []types.ObjectIdentifier
				for _, fo := range batch {
					objs = append(objs, types.ObjectIdentifier{
						Key:       aws.String(fo.Key),
						VersionId: aws.String(fo.VersionId),
					})
				}
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
				defer cancel()
				input := &s3.DeleteObjectsInput{
					Bucket:                    aws.String(bucket),
					Delete:                    &types.Delete{Objects: objs},
					BypassGovernanceRetention: aws.Bool(true),
				}
				resp, err := client.DeleteObjects(ctx, input)
				mu.Lock()
				if err != nil {
					errorCount += len(objs)
					for _, obj := range objs {
						stillFailed = append(stillFailed, FailedObject{
							Key:       aws.ToString(obj.Key),
							VersionId: aws.ToString(obj.VersionId),
							Code:      "RetryFailed",
							Message:   err.Error(),
						})
					}
					mu.Unlock()
					<-retrySem
					return
				}
				deletedCount += len(resp.Deleted)
				errorCount += len(resp.Errors)
				_ = bar.Add(len(resp.Deleted))
				for _, e := range resp.Errors {
					stillFailed = append(stillFailed, FailedObject{
						Key:       aws.ToString(e.Key),
						VersionId: aws.ToString(e.VersionId),
						Code:      aws.ToString(e.Code),
						Message:   aws.ToString(e.Message),
					})
				}
				mu.Unlock()
				<-retrySem
			}(batch)
		}
		retryWg.Wait()
		// Write failures.csv
		if len(stillFailed) > 0 {
			fcsv, ferr := os.Create("failures.csv")
			if ferr == nil {
				writer := csv.NewWriter(fcsv)
				writer.Write([]string{"Key", "VersionId", "Code", "Message"})
				for _, fo := range stillFailed {
					writer.Write([]string{fo.Key, fo.VersionId, fo.Code, fo.Message})
				}
				writer.Flush()
				fcsv.Close()
				log.Printf("Exported %d undeletable objects in failures.csv", len(stillFailed))
			}
		}
	}

	log.Printf("Total objects deleted: %d | Total errors: %d", deletedCount, errorCount)
	fmt.Printf("Total objects deleted: %d | Total errors: %d\n", deletedCount, errorCount)

	// Write total objects detected in snapshot_total.log
	err = os.WriteFile("snapshot_total.log", []byte(fmt.Sprintf("Total objects: %d\n", totalObjects)), 0644)
	if err != nil {
		log.Printf("Error writing total snapshot: %v\n", err)
	}
	saveState()
	duration := time.Since(start)
	fmt.Printf("Total execution time: %v\n", duration)
	log.Printf("Total execution time: %v\n", duration)
	fmt.Printf("Bucket %q fully processed.\n", bucket)
	log.Printf("Bucket %q fully processed.\n", bucket)
}
