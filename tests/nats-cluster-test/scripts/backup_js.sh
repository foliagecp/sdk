#!/bin/sh

# Configuration
SCRIPTS_DIR="/scripts"
BACKUP_DIR="$SCRIPTS_DIR/backups/foliage_jetstream"
LOG_FILE="$SCRIPTS_DIR/logs/foliage_backup.log"
RETENTION_DAYS=30
NATS_USER="nats"
NATS_PASSWORD="foliage"
NATS_PORT="4222"

# List of all NATS cluster nodes
NATS_SERVERS="nats1 nats2 nats3"

# Handle command line arguments
while getopts "d:r:p:h" opt; do
  case $opt in
    d) BACKUP_DIR="$OPTARG" ;;
    r) RETENTION_DAYS="$OPTARG" ;;
    p) NATS_PORT="$OPTARG" ;;
    h) echo "Usage: $0 [-d backup_dir] [-r retention_days] [-p port]"; exit 0 ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# Logging function
log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$LOG_FILE"
}

# Create directories
mkdir -p "$BACKUP_DIR"
mkdir -p "$(dirname $LOG_FILE)"

# Create backup
timestamp=$(date +%Y%m%d_%H%M%S)
backup_path="${BACKUP_DIR}/foliage_backup_${timestamp}"
mkdir -p "$backup_path"

# Try to backup using each server in the cluster
backup_successful=0
for server in $NATS_SERVERS; do
    NATS_URL="nats://${NATS_USER}:${NATS_PASSWORD}@${server}:${NATS_PORT}"

    log "Trying JetStream backup using server $server"

    # Try to run backup command with this server
    if nats account backup "$backup_path" --force --server="$NATS_URL"; then
        log "Backup created in $backup_path using server $server"
        backup_successful=1
        break
    else
        log "WARNING: Failed to create backup using server $server, trying next server..."
    fi
done

# Check if any backup attempt was successful
if [ $backup_successful -eq 1 ]; then
    # Archive the backup
    log "Archiving backup..."
    if tar -czf "${backup_path}.tgz" -C "$BACKUP_DIR" "foliage_backup_${timestamp}"; then
        log "Backup archived to ${backup_path}.tgz"

        # Remove temporary directory
        rm -rf "$backup_path"

        # Clean up old backups
        log "Removing backups older than $RETENTION_DAYS days..."
        find "$BACKUP_DIR" -name "foliage_backup_*.tgz" -type f -mtime +$RETENTION_DAYS -delete

        # Report success
        backup_size=$(du -h "${backup_path}.tgz" | cut -f1)
        log "Backup completed successfully. Size: $backup_size"
    else
        log "ERROR: Failed to create archive"
        exit 1
    fi
else
    log "ERROR: Failed to create backup on any NATS server in the cluster"
    rm -rf "$backup_path"
    exit 1
fi