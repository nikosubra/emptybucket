# EmptyBucket

**EmptyBucket** is a Go-based utility to **empty a versioned S3 bucket** (compatible with AWS and NetApp ONTAP S3) by deleting all object versions and delete markers in parallel with performance tracking and logging.

---

## 🚀 Features

- Deletes **all versions** and **delete markers** from an S3 bucket
- Fully compatible with **AWS SDK v2**
- Asks for credentials and config **at runtime**
- Supports **NetApp ONTAP S3**
- Shows:
  - Total objects
  - Percentage completion
  - Estimated Time Remaining (ETA)
  - Total runtime
- Logs:
  - Progress logs
  - One snapshot progress file (`snapshot_progress.log`)
  - One-time total count (`snapshot_total_objects.log`)
- Handles:
  - Errors and retries
  - ServiceUnavailable throttling
  - Concurrency control via worker pool

---

## 🛠️ Requirements

- Go 1.21+
- Internet connectivity to access the S3-compatible endpoint
- S3 bucket with proper permissions

---

## 🔧 Configuration (Runtime Prompts)

When you run the program, it will prompt you for:

- AWS Access Key ID
- AWS Secret Access Key
- AWS Region
- S3 Endpoint (e.g. `https://s3-extreme.svc.it`)
- Bucket name

Example:
```bash
$ go run emptybucket.go
Enter AWS Access Key ID: ****************
Enter AWS Secret Access Key: ****************
Enter AWS Region: us-east-1
Enter S3 Endpoint URL: https://s3-extreme.svc.it
Enter Bucket Name: backups3dcretelit

📦 How to Use
	1.	Clone or copy the project to your machine.
	2.	Run the script:
      go run emptybucket.go

  3.	Follow the prompts and monitor logs:
      •	Terminal will show progress, ETA, and stats.
      •	Output logs will be written in:
      •	output.log — full execution log
      •	snapshot_progress.log — last status snapshot (overwritten)
      •	snapshot_total_objects.log — stores the total object count
  
📂 Files Generated
output.log -> Main log with progress and errors
snapshot_progress.log -> Last recorded state snapshot
snapshot_total_objects.log -> Total number of objects at start


⚙️ Advanced Options (Code Customizable)
	•	batchSize (default: 500)
	•	maxWorkers (default: 20)
	•	Log frequency (every 20s)
	•	Retry logic on 503 errors
	•	Dry-run mode (optional: disabled by default)

🛑 Disclaimer

Use responsibly. This will irreversibly delete data in the specified S3 bucket.

📃 License
MIT License




# emptybucket
