#!/bin/bash
#set -x
# foliage_mgr.sh - Tool for Foliage NATS JetStream backup and restoration

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="${SCRIPT_DIR}/backups"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/foliage_mgr.log"
RETENTION_DAYS=180
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yaml"
DATA_DIR="${SCRIPT_DIR}/data"
CONTAINER_BACKUP_DIR="/backups"
DEBUG=true

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
    archive_name="fol_backup_${timestamp}"
    container_backup_path="${CONTAINER_BACKUP_DIR}/${backup_name}"
    container_archive_path="${CONTAINER_BACKUP_DIR}/${archive_name}"

    log "Creating backup and archive directories in container"
    docker compose -f "$COMPOSE_FILE" exec io \
        sh -c "mkdir -p '${container_backup_path}' '${container_archive_path}'" || {
        log "ERROR: Failed to create directories"
        exit 1
    }

    docker compose -f "$COMPOSE_FILE" exec io chmod 755 "${container_backup_path}" "${container_archive_path}"

    log "Creating JetStream backup"
    if ! nats_cmd "account backup" "${container_backup_path} --force"; then
        log "ERROR: Backup command failed"
        exit 1
    fi

    log "Archiving KV stores"
    if ! docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "cd '${container_backup_path}' && \
        find . -type d -name 'KV_*' | tar -czf '${container_archive_path}/kv.tgz' -T -"; then
        log "ERROR: KV archive failed"
        exit 1
    fi

    log "Archiving other streams"
    if ! docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "cd '${container_backup_path}' && \
        find . -type d -name 'KV_*' -prune -o -type f -print | tar -czf '${container_archive_path}/streams.tgz' -T -"; then
        log "ERROR: Other streams archive failed"
        exit 1
    fi

    log "Verifying archives in target directory"
    if ! docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "ls '${container_archive_path}/kv.tgz' '${container_archive_path}/streams.tgz'"; then
        log "ERROR: Archives not found in target directory"
        exit 1
    fi

    log "Cleaning up temporary backup directory"
    docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "rm -rf '${container_backup_path}'"

    log "Cleaning old archives"
    find "${BACKUP_DIR}" -type d -name "fol_backup_*" -mtime +${RETENTION_DAYS} -exec rm -rf {} \;

    kv_size=$(docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "du -h '${container_archive_path}/kv.tgz' | cut -f1")
    streams_size=$(docker compose -f "$COMPOSE_FILE" exec io sh -c \
        "du -h '${container_archive_path}/streams.tgz' | cut -f1")

    log "Backup completed successfully. Archive directory: ${archive_name}, Sizes: KV=${kv_size}, Streams=${streams_size}"
}

# Restore function
do_restore() {
    local backup_path="$1"
    local restore_type="all"
    local available_backups=()

    mapfile -t available_backups < <(find "$BACKUP_DIR" -maxdepth 1 -type d -name "fol_backup_*" | sort -r)

    if [ -z "$backup_path" ]; then
        if [ ${#available_backups[@]} -eq 0 ]; then
            log "No backups found in $BACKUP_DIR"
            exit 1
        fi

        PS3="Select backup to restore: "
        select selected_backup in "${available_backups[@]##*/}"; do
            [ -n "$selected_backup" ] && break
        done
        backup_path="$BACKUP_DIR/$selected_backup"
    fi

    if [ ! -f "$backup_path/kv.tgz" ] || [ ! -f "$backup_path/streams.tgz" ]; then
        log "Invalid backup structure in $backup_path"
        exit 1
    fi

    echo
    PS3="Select restore type: "
    select restore_type in "All" "KV-only" "Streams-only"; do
        case $REPLY in
            1|2|3) break ;;
            *) echo "Invalid option" ;;
        esac
    done

    log "Starting restore from $(basename "$backup_path") ($restore_type)"

    log "Stopping services..."
    docker compose -f "$COMPOSE_FILE" down

    log "Starting NATS cluster..."
    docker compose -f "$COMPOSE_FILE" up -d nats1 nats2 nats3 io
    sleep 10

    local temp_restore_dir
    temp_restore_dir=$(docker compose -f "$COMPOSE_FILE" exec io mktemp -d)
    trap 'docker compose -f "$COMPOSE_FILE" exec io rm -rf "$temp_restore_dir"' EXIT

    mapfile -t streams < <(nats_cmd "stream ls -n" | grep -v "^KV_" | grep -v "^$" | grep -v "^\[" || echo "")
    mapfile -t kv_buckets < <(nats_cmd "stream ls -n" | grep "^KV_" | grep -v "^$" || echo "")

    debug "Streams found: ${streams[*]:-none}"
    debug "KV buckets found: ${kv_buckets[*]:-none}"

    case $restore_type in
        "All")
            for s in "${streams[@]}"; do
                if [[ -z "$s" || "$s" == "null" ]]; then
                    log "WARN: Skipping invalid stream name: '$s'"
                    continue
                fi
                if ! nats_cmd "stream rm -f" "$s"; then
                    log "ERROR: Clearing stream failed: $s"
                    exit 1
                fi
            done

            if [[ ${#kv_buckets[@]} -ne 0 ]]; then
                for kv in "${kv_buckets[@]}"; do
                    if [[ -z "$kv" || "$kv" == "null" ]]; then
                        log "WARN: Skipping invalid KV bucket name: '$kv'"
                        continue
                    fi

                    if ! nats_cmd "stream rm -f" "$kv"; then
                        log "ERROR: Clearing KV bucket failed: $kv"
                        exit 1
                    fi
                done
            else
                log "No KV buckets to remove"
            fi


            docker compose -f "$COMPOSE_FILE" exec io tar -xzf "$CONTAINER_BACKUP_DIR/$(basename "$backup_path")/kv.tgz" -C "$temp_restore_dir"
            docker compose -f "$COMPOSE_FILE" exec io tar -xzf "$CONTAINER_BACKUP_DIR/$(basename "$backup_path")/streams.tgz" -C "$temp_restore_dir"
            ;;
        "KV-only")
            if [[ ${#kv_buckets[@]} -ne 0 ]]; then
                for kv in "${kv_buckets[@]}"; do
                    if [[ -z "$kv" || "$kv" == "null" ]]; then
                        log "WARN: Skipping invalid KV bucket name: '$kv'"
                        continue
                    fi

                    if ! nats_cmd "stream rm -f" "$kv"; then
                        log "ERROR: Clearing KV bucket failed: $kv"
                        exit 1
                    fi
                done
            else
                log "No KV buckets to remove"
            fi

            docker compose -f "$COMPOSE_FILE" exec io tar -xzf "$CONTAINER_BACKUP_DIR/$(basename "$backup_path")/kv.tgz" -C "$temp_restore_dir"
            ;;
        "Streams-only")
            for s in "${streams[@]}"; do
                if [[ -z "$s" || "$s" == "null" ]]; then
                    log "WARN: Skipping invalid stream name: '$s'"
                    continue
                fi
                if ! nats_cmd "stream rm -f" "$s"; then
                    log "ERROR: Clearing stream failed: $s"
                    exit 1
                fi
            done

            docker compose -f "$COMPOSE_FILE" exec io tar -xzf "$CONTAINER_BACKUP_DIR/$(basename "$backup_path")/streams.tgz" -C "$temp_restore_dir"
            ;;
    esac

    log "Restoring JetStream data..."
    if ! nats_cmd "account restore" "$temp_restore_dir"; then
        log "ERROR: Restore failed"
        exit 1
    fi

    log "Finalizing..."
    docker compose -f "$COMPOSE_FILE" up -d
    log "Restore completed successfully"
}

# List backups
list_backups() {
    log "Available backup directories:"

    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
        log "No backups found."
        return
    fi

    printf "\n%-30s %-15s %-20s\n" "Backup Date" "Size" "Directory Name"
    printf "%-30s %-15s %-20s\n" "$(printf '%0.s-' {1..30})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..50})"

    find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d -name "fol_backup_*" | sort -r | while read backup_dir; do
        dirname=$(basename "$backup_dir")
        size=$(du -sh "$backup_dir" | cut -f1)
        date_str=$(echo "$dirname" | sed -E 's/fol_backup_([0-9]{8})_([0-9]{6})/\1-\2/' | sed -E 's/([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5:\6/')

        printf "%-30s %-15s %-20s\n" "$date_str" "$size" "$dirname" | tee -a "$LOG_FILE"
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
    echo "  --retention DAYS      Set backup retention period in days (default: $RETENTION_DAYS)"
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
            --retention)
                RETENTION_DAYS="$2"
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
        *)
            log "No valid action specified"
            show_help
            exit 1
            ;;
    esac

    log "Operation completed successfully"
}

main "$@"