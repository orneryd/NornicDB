# Backup & Restore

**Protect your data with regular backups.**

## Backup Methods

| Method        | Use Case     | Downtime |
| ------------- | ------------ | -------- |
| Online Backup | Production   | None     |
| Snapshot      | VM/Container | Seconds  |
| File Copy     | Development  | Yes      |

## WAL Compaction ⭐ NEW

NornicDB's Write-Ahead Log (WAL) ensures durability but can grow unbounded without compaction.
Starting in v1.0.0, NornicDB includes automatic WAL compaction.

### Enable Auto-Compaction (Recommended)

```go
// Enable automatic snapshots + truncation every 5 minutes
wal.EnableAutoCompaction("/data/snapshots")

// Disable when shutting down
wal.DisableAutoCompaction()
```

### Manual Compaction

```go
// Create snapshot first
snapshotSeq, err := wal.CreateSnapshot("/data/snapshots")
if err != nil {
    log.Fatal(err)
}

// Truncate WAL entries before the snapshot
removed, err := wal.TruncateAfterSnapshot(snapshotSeq)
if err != nil {
    log.Fatal(err)
}
log.Printf("Removed %d WAL entries", removed)
```

### Benefits

| Metric        | Without Compaction        | With Compaction       |
| ------------- | ------------------------- | --------------------- |
| Disk Usage    | Unbounded growth          | ~10MB typical         |
| Recovery Time | Minutes to hours          | Milliseconds          |
| I/O Load      | High (replaying full WAL) | Low (recent snapshot) |

### Monitoring

```go
stats := wal.GetSnapshotStats()
fmt.Printf("Last snapshot: %s\n", stats.LastSnapshotTime)
fmt.Printf("Entries since snapshot: %d\n", stats.EntriesSinceSnapshot)
fmt.Printf("Disk saved: %s\n", stats.DiskSavings)
```

## Online Backup

### Create Backup

```bash
# Backup to file
nornicdb backup --output backup-$(date +%Y%m%d).tar.gz

# Backup with compression
nornicdb backup --output backup.tar.gz --compress gzip

# Backup specific data
nornicdb backup --output backup.tar.gz --include-wal
```

**Note:** All backups automatically include user accounts stored in the system database. When you restore from backup, all user accounts are restored along with the database data.

### API Backup

```bash
curl -X POST http://localhost:7474/admin/backup \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"output": "/backups/backup-2024-12-01.tar.gz"}'
```

## Restore

### Restore from JSON Backup

```go
// Restore from JSON backup file
err := db.Restore(ctx, "backup-20241201.json")
if err != nil {
    log.Fatal(err)
}
```

### API Restore

```bash
curl -X POST http://localhost:7474/admin/restore \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"input": "/backups/backup-2024-12-01.json"}'
```

### Verify Restore

```bash
# Check node count
curl http://localhost:7474/status -H "Authorization: Bearer $TOKEN"
```

### Note on Backup Format

NornicDB uses JSON backup format which is portable across different storage backends.
For production BadgerDB deployments, use the storage-level backup commands for
incremental backups with better performance.

## Docker Backup

### Backup Volume

```bash
# Create backup
docker run --rm \
  -v nornicdb-data:/data:ro \
  -v $(pwd):/backup \
  busybox tar czf /backup/nornicdb-backup.tar.gz /data
```

### Restore Volume

```bash
# Stop container
docker stop nornicdb

# Restore backup
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  busybox tar xzf /backup/nornicdb-backup.tar.gz -C /

# Start container
docker start nornicdb
```

## Kubernetes Backup

### Using VolumeSnapshot

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: nornicdb-snapshot-20241201
spec:
  volumeSnapshotClassName: csi-snapshotter
  source:
    persistentVolumeClaimName: nornicdb-pvc
```

### Restore from Snapshot

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nornicdb-pvc-restored
spec:
  dataSource:
    name: nornicdb-snapshot-20241201
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

## Automated Backups

### Cron Job

```bash
# /etc/cron.d/nornicdb-backup
0 2 * * * root /usr/local/bin/nornicdb backup --output /backups/nornicdb-$(date +\%Y\%m\%d).tar.gz
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nornicdb-backup
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: timothyswt/nornicdb-arm64-metal:latest
              command:
                - /app/nornicdb
                - backup
                - --output=/backups/backup-$(date +%Y%m%d).tar.gz
              volumeMounts:
                - name: data
                  mountPath: /data
                  readOnly: true
                - name: backups
                  mountPath: /backups
          volumes:
            - name: data
              persistentVolumeClaim:
                claimName: nornicdb-pvc
            - name: backups
              persistentVolumeClaim:
                claimName: nornicdb-backups-pvc
          restartPolicy: OnFailure
```

## Retention Policy

### Automatic Cleanup

```bash
# Keep last 7 daily backups
find /backups -name "nornicdb-*.tar.gz" -mtime +7 -delete

# Keep last 4 weekly backups
find /backups/weekly -name "*.tar.gz" -mtime +28 -delete
```

### Retention Script

```bash
#!/bin/bash
# backup-rotate.sh

BACKUP_DIR=/backups
DAILY_KEEP=7
WEEKLY_KEEP=4
MONTHLY_KEEP=12

# Create backup
nornicdb backup --output $BACKUP_DIR/daily/nornicdb-$(date +%Y%m%d).tar.gz

# Rotate daily
find $BACKUP_DIR/daily -name "*.tar.gz" -mtime +$DAILY_KEEP -delete

# Weekly (Sunday)
if [ $(date +%u) -eq 7 ]; then
  cp $BACKUP_DIR/daily/nornicdb-$(date +%Y%m%d).tar.gz $BACKUP_DIR/weekly/
fi

# Monthly (1st of month)
if [ $(date +%d) -eq 01 ]; then
  cp $BACKUP_DIR/daily/nornicdb-$(date +%Y%m%d).tar.gz $BACKUP_DIR/monthly/
fi
```

## Cloud Backup

### AWS S3

```bash
# Backup to S3
nornicdb backup --output - | aws s3 cp - s3://mybucket/nornicdb/backup-$(date +%Y%m%d).tar.gz

# Restore from S3
aws s3 cp s3://mybucket/nornicdb/backup-20241201.tar.gz - | nornicdb restore --input -
```

### Google Cloud Storage

```bash
# Backup to GCS
nornicdb backup --output - | gsutil cp - gs://mybucket/nornicdb/backup-$(date +%Y%m%d).tar.gz
```

## Disaster Recovery

### Recovery Steps

1. **Assess** - Determine extent of data loss
2. **Provision** - Create new infrastructure if needed
3. **Restore** - Restore from most recent backup
4. **Verify** - Check data integrity
5. **Resume** - Bring system back online

### Recovery Time Objectives

| Backup Type | RTO        | RPO        |
| ----------- | ---------- | ---------- |
| Online      | < 1 hour   | < 1 hour   |
| Daily       | < 4 hours  | < 24 hours |
| Weekly      | < 24 hours | < 7 days   |

## Troubleshooting

### Backup Fails

```bash
# Check disk space
df -h /backups

# Check permissions
ls -la /backups

# Check logs
journalctl -u nornicdb -n 100
```

### Restore Fails

```bash
# Verify backup integrity
tar tzf backup.tar.gz > /dev/null && echo "OK"

# Check data directory permissions
ls -la /var/lib/nornicdb
```

## See Also

- **[Deployment](deployment.md)** - Deployment guide
- **[Monitoring](monitoring.md)** - Health monitoring
- **[Troubleshooting](troubleshooting.md)** - Common issues
