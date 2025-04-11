#!/bin/bash
# foliage_mgr.sh - Tool for Foliage NATS JetStream backup and restoration

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="${SCRIPT_DIR}/backups"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/foliage_mgr.log"
RETENTION_DAYS=30
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yaml"
DATA_DIR="${SCRIPT_DIR}/data"
CRON_SCHEDULE="0 2 * * *"  # Default: 2:00 AM daily
CONTAINER_BACKUP_DIR="/backups"
DEBUG=false

# Load environment variables if .env file exists
ENV_FILE="${SCRIPT_DIR}/configs/.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

# Set default NATS credentials if not already set
NATS_USER=${NATS_USERNAME:-${NATS_USER:-"nats"}}
NATS_PASSWORD=${NATS_PASSWORD:-"foliage"}
NATS_PORT="4222"

# Ensure required directories exist
mkdir -p "$LOG_DIR" "$BACKUP_DIR"
chmod 755 "$BACKUP_DIR"

log() {
    local message="$1"
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] ${message}" | tee -a "$LOG_FILE"
}

debug() {
    if [ "$DEBUG" = true ]; then
        log "[DEBUG] $1"
    fi
}

nats_cmd() {
    local cmd="$1"
    local args="$2"
    local auth_str=""

    [ -n "$NATS_USER" ] && [ -n "$NATS_PASSWORD" ] && auth_str="${NATS_USER}:${NATS_PASSWORD}@"

    local server_urls="nats://${auth_str}nats1:4222,nats://${auth_str}nats2:4222,nats://${auth_str}nats3:4222"

    debug "Executing NATS command: nats $cmd $args --server=[URLs]"

    docker compose -f "$COMPOSE_FILE" exec io nats $cmd $args --server="$server_urls"
    return $?
}

# Backup function
do_backup() {
    log "Starting Foliage JetStream backup"

    if ! docker compose -f "$COMPOSE_FILE" ps | grep -q "nats[1-3].*Up"; then
        log "Starting NATS services..."
        docker compose -f "$COMPOSE_FILE" up -d nats1 nats2 nats3 io
        sleep 10
    fi

    if ! docker compose -f "$COMPOSE_FILE" ps | grep -q "io.*Up"; then
        log "Starting io container..."
        docker compose -f "$COMPOSE_FILE" up -d io
        sleep 5
    fi

    timestamp=$(date +%Y%m%d_%H%M%S)
    backup_name="foliage_backup_${timestamp}"
    container_backup_path="${CONTAINER_BACKUP_DIR}/${backup_name}"

    log "Creating backup directory in container"
    docker compose -f "$COMPOSE_FILE" exec io mkdir -p "$container_backup_path" || {
        log "ERROR: Failed to create backup directory in container"
        exit 1
    }

    docker compose -f "$COMPOSE_FILE" exec io chmod 755 "$container_backup_path"

    log "Creating backup at $container_backup_path using 'nats account backup'"
    if ! nats_cmd "account backup" "$container_backup_path --force"; then
        log "ERROR: Backup command failed"
        exit 1
    fi

    # Create archive of backup
    log "Creating archive..."
    if ! docker compose -f "$COMPOSE_FILE" exec io tar -czf "${container_backup_path}.tgz" -C "$CONTAINER_BACKUP_DIR" "$backup_name"; then
        log "ERROR: Failed to create archive"
        exit 1
    fi

    docker compose -f "$COMPOSE_FILE" exec io chmod 644 "${container_backup_path}.tgz"

    if ! docker compose -f "$COMPOSE_FILE" exec io ls -la "${container_backup_path}.tgz" >/dev/null 2>&1; then
        log "ERROR: Archive not created"
        exit 1
    fi

    docker compose -f "$COMPOSE_FILE" exec io rm -rf "$container_backup_path"

    # Clean up old backups
    find "$BACKUP_DIR" -name "foliage_backup_*.tgz" -type f -mtime +$RETENTION_DAYS -delete

    if [ -f "${BACKUP_DIR}/${backup_name}.tgz" ]; then
        backup_size=$(du -h "${BACKUP_DIR}/${backup_name}.tgz" | cut -f1)
        log "Backup completed successfully. Size: $backup_size"
    else
        log "ERROR: Backup archive not found on host. Check volume mount configuration."
        exit 1
    fi
}

# Restore function
do_restore() {
    local backup_file="$1"
    local backup_name

    if [ -z "$backup_file" ]; then
        backup_file=$(find "$BACKUP_DIR" -name "foliage_backup_*.tgz" -type f -printf "%T@ %p\n" | sort -n | tail -1 | cut -d' ' -f2-)

        if [ -z "$backup_file" ]; then
            log "No backup files found"
            exit 1
        fi
        log "Using most recent backup: $(basename "$backup_file")"
    elif [ ! -f "$backup_file" ]; then
        log "Backup file not found: $backup_file"
        exit 1
    fi

    backup_name=$(basename "$backup_file" .tgz)
    log "Starting restore process from $backup_name..."

    log "Stopping all services"
    docker compose -f "$COMPOSE_FILE" down

    log "Clearing existing JetStream data"
    rm -rf "${DATA_DIR}/jetstream"*
    mkdir -p "${DATA_DIR}/jetstream1" "${DATA_DIR}/jetstream2" "${DATA_DIR}/jetstream3"

    log "Starting NATS services"
    docker compose -f "$COMPOSE_FILE" up -d nats1 nats2 nats3 io
    sleep 10

    container_restore_dir="${CONTAINER_BACKUP_DIR}/restore_${backup_name}"

    docker compose -f "$COMPOSE_FILE" exec io mkdir -p "$container_restore_dir"
    docker compose -f "$COMPOSE_FILE" exec io chmod 755 "$container_restore_dir"

    log "Extracting backup archive"
    if ! docker compose -f "$COMPOSE_FILE" exec io tar -xzf "${CONTAINER_BACKUP_DIR}/${backup_name}.tgz" -C "$container_restore_dir"; then
        log "ERROR: Failed to extract backup in container"
        exit 1
    fi

    backup_content_dir=$(docker compose -f "$COMPOSE_FILE" exec io find "$container_restore_dir" -type d -name "foliage_backup_*" | head -1)

    if [ -z "$backup_content_dir" ]; then
        # If we didn't find a nested directory, use the extract directory itself
        backup_content_dir="${container_restore_dir}/${backup_name}"
    fi

    log "Restoring data from $backup_content_dir"
    if ! nats_cmd "account restore" "$backup_content_dir"; then
        log "ERROR: Data restore failed"
        exit 1
    fi

    docker compose -f "$COMPOSE_FILE" exec io rm -rf "$container_restore_dir"

    log "Restarting all services"
    docker compose -f "$COMPOSE_FILE" up -d

    log "Restore completed successfully"
}

# Schedule backup function
schedule_backup() {
    log "Setting up scheduled backup"

    cron_cmd="$SCRIPT_DIR/$(basename "$0") --backup"

    (crontab -l 2>/dev/null | grep -v "$cron_cmd") | crontab -

    (crontab -l 2>/dev/null; echo "$CRON_SCHEDULE $cron_cmd") | crontab -

    log "Backup scheduled: $CRON_SCHEDULE"
}

# List backups
list_backups() {
    log "Available backups:"

    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
        log "No backups found."
        return
    fi

    printf "\n%-30s %-15s %-20s\n" "Backup Date" "Size" "Filename"
    printf "%-30s %-15s %-20s\n" "$(printf '%0.s-' {1..30})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..50})"

    find "$BACKUP_DIR" -name "foliage_backup_*.tgz" -type f | while read backup; do
        filename=$(basename "$backup")
        size=$(du -h "$backup" | cut -f1)
        date_str=$(echo "$filename" | sed -E 's/foliage_backup_([0-9]{8})_([0-9]{6})\.tgz/\1-\2/' | sed -E 's/([0-9]{4})([0-9]{2})([0-9]{2})-([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5:\6/')

        printf "%-30s %-15s %-20s\n" "$date_str" "$size" "$filename"
    done
    printf "\n"
}

# Show help
show_help() {
    echo "Foliage NATS JetStream Backup & Restore Tool"
    echo ""
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --backup              Perform a backup"
    echo "  --restore [FILE]      Restore from backup (uses latest if FILE not specified)"
    echo "  --list                List available backups"
    echo "  --schedule            Set up scheduled backups via cron"
    echo "  --retention DAYS      Set backup retention period in days (default: $RETENTION_DAYS)"
    echo "  --cron 'EXPR'         Set cron schedule expression (default: '$CRON_SCHEDULE')"
    echo "  --debug               Enable debug mode"
    echo "  --help                Show this help message"
}

# Main function
main() {
    # If no arguments provided, show help
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi

    action=""
    restore_file=""

    while [ $# -gt 0 ]; do
        case "$1" in
            --backup)
                action="backup"
                shift
                ;;
            --restore)
                action="restore"
                if [ $# -gt 1 ] && [[ ! "$2" == --* ]]; then
                    restore_file="$2"
                    shift
                fi
                shift
                ;;
            --list)
                action="list"
                shift
                ;;
            --schedule)
                action="schedule"
                shift
                ;;
            --retention)
                RETENTION_DAYS="$2"
                shift 2
                ;;
            --cron)
                CRON_SCHEDULE="$2"
                shift 2
                ;;
            --debug)
                DEBUG=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    case "$action" in
        backup)
            do_backup
            ;;
        restore)
            do_restore "$restore_file"
            ;;
        list)
            list_backups
            ;;
        schedule)
            schedule_backup
            ;;
        *)
            log "No valid action specified"
            show_help
            exit 1
            ;;
    esac

    log "Operation completed successfully"
}

main "$@"