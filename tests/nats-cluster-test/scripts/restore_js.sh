#!/bin/sh

# Configuration
SCRIPTS_DIR="/scripts"
BACKUP_DIR="$SCRIPTS_DIR/backups/foliage_jetstream"
LOG_FILE="$SCRIPTS_DIR/logs/foliage_restore.log"
NATS_USER="nats"
NATS_PASSWORD="foliage"
NATS_PORT="4222"
TEMP_DIR="/tmp/foliage_restore_temp"

# List of all NATS cluster nodes
NATS_SERVERS="nats1 nats2 nats3"

# Handle command line arguments
while getopts "d:p:b:h" opt; do
  case $opt in
    d) BACKUP_DIR="$OPTARG" ;;
    p) NATS_PORT="$OPTARG" ;;
    b) BACKUP_FILE="$OPTARG" ;;
    h)
      echo "Usage: $0 [-d backup_dir] [-p port] [-b backup_file]"
      echo "  -d: Directory containing backups (default: $BACKUP_DIR)"
      echo "  -p: NATS server port (default: $NATS_PORT)"
      echo "  -b: Specific backup file to restore (if not specified, will use latest)"
      exit 0
      ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# Logging function
log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "$LOG_FILE"
}

# Create directories
mkdir -p "$(dirname $LOG_FILE)"
mkdir -p "$TEMP_DIR"

# Check if backup directory exists
if [ ! -d "$BACKUP_DIR" ]; then
    log "ERROR: Backup directory $BACKUP_DIR does not exist"
    exit 1
fi

# If no specific backup file is provided, find the latest backup
if [ -z "$BACKUP_FILE" ]; then
    BACKUP_FILE=$(find "$BACKUP_DIR" -name "foliage_backup_*.tgz" -type f -print0 | xargs -0 ls -t | head -n1)
    if [ -z "$BACKUP_FILE" ]; then
        log "ERROR: No backup files found in $BACKUP_DIR"
        exit 1
    fi
    log "Using latest backup: $BACKUP_FILE"
else
    # Check if the specified backup file exists
    if [ ! -f "$BACKUP_FILE" ]; then
        log "ERROR: Specified backup file $BACKUP_FILE does not exist"
        exit 1
    fi
    log "Using specified backup: $BACKUP_FILE"
fi

# Extract the backup archive
log "Extracting backup archive..."
if ! tar -xzf "$BACKUP_FILE" -C "$TEMP_DIR"; then
    log "ERROR: Failed to extract backup archive"
    rm -rf "$TEMP_DIR"
    exit 1
fi

# Find the extracted directory
EXTRACTED_DIR=$(find "$TEMP_DIR" -maxdepth 1 -type d -name "foliage_backup_*" | head -n1)
if [ -z "$EXTRACTED_DIR" ]; then
    log "ERROR: Failed to locate extracted backup directory"
    rm -rf "$TEMP_DIR"
    exit 1
fi

log "Extracted backup to: $EXTRACTED_DIR"

# Try to restore using each server in the cluster
log "Starting JetStream restore - will try each server in the cluster"
restore_successful=0

for server in $NATS_SERVERS; do
    NATS_URL="nats://${NATS_USER}:${NATS_PASSWORD}@${server}:${NATS_PORT}"

    log "Trying JetStream restore using server $server"

    # Try to run restore command with this server
    if nats account restore "$EXTRACTED_DIR" --server="$NATS_URL"; then
        log "Restore completed successfully using server $server"
        restore_successful=1
        break
    else
        log "WARNING: Failed to restore using server $server, trying next server..."
    fi
done

# Clean up
log "Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

# Check if any restore attempt was successful
if [ $restore_successful -eq 1 ]; then
    log "Restore process completed successfully"
    exit 0
else
    log "ERROR: Failed to restore JetStream data on any NATS server in the cluster"
    exit 1
fi