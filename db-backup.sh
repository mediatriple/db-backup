#!/usr/bin/env bash

set -u -o pipefail

CONFIG_FILE="${1:-./db-backup.conf}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "❌ Config file not found: $CONFIG_FILE"
  exit 1
fi

# shellcheck disable=SC1090
source "$CONFIG_FILE"

if [[ -z "${BACKUP_DIR:-}" ]]; then
  echo "❌ BACKUP_DIR is missing in config"
  exit 1
fi

if [[ -z "${RETENTION_DAYS:-}" ]]; then
  echo "❌ RETENTION_DAYS is missing in config"
  exit 1
fi

if [[ ${#DATABASES[@]:-0} -eq 0 ]]; then
  echo "❌ No databases defined in DATABASES array"
  exit 1
fi

MAX_PARALLEL_JOBS="${MAX_PARALLEL_JOBS:-1}"
DUMP_DIR="${DUMP_DIR:-$BACKUP_DIR/dumps}"
LOG_DIR="${LOG_DIR:-$BACKUP_DIR/logs}"
LOCK_DIR="${LOCK_DIR:-$BACKUP_DIR/.db-backup.lock}"
PRINT_LOG_TO_CONSOLE="${PRINT_LOG_TO_CONSOLE:-true}"
CONSOLE_PROGRESS_EVERY="${CONSOLE_PROGRESS_EVERY:-5}"

if ! [[ "$MAX_PARALLEL_JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "❌ MAX_PARALLEL_JOBS must be a positive integer"
  exit 1
fi

PRINT_LOG_TO_CONSOLE="$(echo "$PRINT_LOG_TO_CONSOLE" | tr '[:upper:]' '[:lower:]')"
if [[ "$PRINT_LOG_TO_CONSOLE" != "true" && "$PRINT_LOG_TO_CONSOLE" != "false" ]]; then
  echo "❌ PRINT_LOG_TO_CONSOLE must be true or false"
  exit 1
fi

if ! [[ "$CONSOLE_PROGRESS_EVERY" =~ ^[1-9][0-9]*$ ]]; then
  echo "❌ CONSOLE_PROGRESS_EVERY must be a positive integer"
  exit 1
fi

mkdir -p "$DUMP_DIR" "$LOG_DIR"

TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="$LOG_DIR/backup_${TIMESTAMP}.log"

SUCCESS_COUNT=0
FAIL_COUNT=0
PIDS=()

log() {
  local line="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$line" >> "$LOG_FILE"

  if [[ "$PRINT_LOG_TO_CONSOLE" == "true" ]]; then
    echo "$line"
  fi
}

console() {
  # Verbose modda log() zaten konsola yazdığı için çift çıktıyı engelle.
  if [[ "$PRINT_LOG_TO_CONSOLE" == "true" ]]; then
    return 0
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

cleanup_running_jobs() {
  local pid

  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  wait 2>/dev/null || true
}

release_lock() {
  rm -rf "$LOCK_DIR" 2>/dev/null || true
}

acquire_lock() {
  local existing_pid

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" > "$LOCK_DIR/pid"
    return 0
  fi

  if [[ -f "$LOCK_DIR/pid" ]]; then
    existing_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
      echo "❌ Another backup process is already running (PID: $existing_pid)"
      return 1
    fi
  fi

  rm -rf "$LOCK_DIR" 2>/dev/null || true
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" > "$LOCK_DIR/pid"
    return 0
  fi

  echo "❌ Could not create lock directory: $LOCK_DIR"
  return 1
}

handle_interrupt() {
  log "🛑 Backup interrupted. Stopping running jobs..."
  console "🛑 Backup interrupted. Stopping running jobs..."
  cleanup_running_jobs
  exit 130
}

trap handle_interrupt INT TERM
trap release_lock EXIT

is_system_database() {
  local db_name="$1"

  case "$db_name" in
    information_schema|mysql|performance_schema|sys)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

resolve_database_entries() {
  local entry="$1"
  local BACKUP_NAME DB_HOST DB_PORT DB_NAME DB_USER DB_PASS EXCLUDED_TABLES MYSQL_EXTRA_OPTIONS
  local discovered_databases_raw
  local discovered_db
  local discovered_count=0
  local option
  local -a MYSQL_OPTION_ARGS
  local -a CLEAN_MYSQL_OPTION_ARGS
  local -a MYSQL_DISCOVERY_CMD

  IFS='|' read -r BACKUP_NAME DB_HOST DB_PORT DB_NAME DB_USER DB_PASS EXCLUDED_TABLES MYSQL_EXTRA_OPTIONS <<< "$entry"

  if [[ -z "${DB_HOST:-}" || -z "${DB_PORT:-}" || -z "${DB_NAME:-}" || -z "${DB_USER:-}" || -z "${DB_PASS:-}" ]]; then
    log "⚠️ Skipping invalid entry: $entry" >&2
    return 1
  fi

  if [[ "$DB_NAME" != "*" ]]; then
    printf '%s\n' "$entry"
    return 0
  fi

  log "🔎 Discovering databases for user: $DB_USER ($DB_HOST:$DB_PORT)" >&2
  console "🔎 Discovering databases for user: $DB_USER ($DB_HOST:$DB_PORT)" >&2
  MYSQL_OPTION_ARGS=()
  CLEAN_MYSQL_OPTION_ARGS=()
  if [[ -n "${MYSQL_EXTRA_OPTIONS:-}" ]]; then
    IFS=',' read -ra MYSQL_OPTION_ARGS <<< "$MYSQL_EXTRA_OPTIONS"
    for option in "${MYSQL_OPTION_ARGS[@]}"; do
      option="$(echo "$option" | xargs)"
      [[ -z "$option" ]] && continue
      CLEAN_MYSQL_OPTION_ARGS+=("$option")
    done
  fi

  MYSQL_DISCOVERY_CMD=(
    mysql
    --host="$DB_HOST"
    --port="$DB_PORT"
    --user="$DB_USER"
    --batch
    --skip-column-names
    --silent
  )
  if [[ ${#CLEAN_MYSQL_OPTION_ARGS[@]} -gt 0 ]]; then
    MYSQL_DISCOVERY_CMD+=("${CLEAN_MYSQL_OPTION_ARGS[@]}")
  fi
  MYSQL_DISCOVERY_CMD+=('--execute=SHOW DATABASES;')

  if ! discovered_databases_raw="$(
    MYSQL_PWD="$DB_PASS" "${MYSQL_DISCOVERY_CMD[@]}" 2>>"$LOG_FILE"
  )"; then
    log "❌ Failed to discover databases for $DB_USER on $DB_HOST:$DB_PORT" >&2
    return 1
  fi

  while IFS= read -r discovered_db; do
    [[ -z "$discovered_db" ]] && continue

    if is_system_database "$discovered_db"; then
      continue
    fi

    printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
      "$BACKUP_NAME" "$DB_HOST" "$DB_PORT" "$discovered_db" "$DB_USER" "$DB_PASS" "$EXCLUDED_TABLES" "$MYSQL_EXTRA_OPTIONS"
    ((discovered_count++))
  done <<< "$discovered_databases_raw"

  if [[ "$discovered_count" -eq 0 ]]; then
    log "⚠️ No databases discovered for $DB_USER on $DB_HOST:$DB_PORT" >&2
    console "⚠️ No databases discovered for $DB_USER on $DB_HOST:$DB_PORT" >&2
    return 1
  fi

  log "🧾 Discovered $discovered_count database(s) for $DB_USER on $DB_HOST:$DB_PORT" >&2
  console "🧾 Discovered $discovered_count database(s) for $DB_USER on $DB_HOST:$DB_PORT" >&2
}

backup_database() {
  local entry="$1"
  local BACKUP_NAME DB_HOST DB_PORT DB_NAME DB_USER DB_PASS EXCLUDED_TABLES MYSQL_EXTRA_OPTIONS
  local BACKUP_FILE COMPRESSED_FILE
  local DB_LOG_FILE SAFE_HOST SAFE_USER SAFE_BACKUP_NAME SOURCE_KEY SOURCE_DUMP_DIR SOURCE_LOG_DIR
  local option
  local -a IGNORE_TABLE_ARGS
  local -a MYSQL_OPTION_ARGS
  local -a CLEAN_MYSQL_OPTION_ARGS
  local -a TABLE_LIST
  local -a DUMP_CMD
  local table

  IFS='|' read -r BACKUP_NAME DB_HOST DB_PORT DB_NAME DB_USER DB_PASS EXCLUDED_TABLES MYSQL_EXTRA_OPTIONS <<< "$entry"

  if [[ -z "${DB_HOST:-}" || -z "${DB_PORT:-}" || -z "${DB_NAME:-}" || -z "${DB_USER:-}" || -z "${DB_PASS:-}" ]]; then
    log "⚠️ Skipping invalid entry: $entry"
    return 1
  fi

  SAFE_HOST="${DB_HOST//[^[:alnum:]]/_}"
  SAFE_USER="${DB_USER//[^[:alnum:]]/_}"
  BACKUP_NAME="$(echo "${BACKUP_NAME:-}" | xargs)"
  SAFE_BACKUP_NAME="${BACKUP_NAME//[^[:alnum:]_-]/_}"
  if [[ -n "$SAFE_BACKUP_NAME" ]]; then
    SOURCE_KEY="$SAFE_BACKUP_NAME"
  else
    SOURCE_KEY="${SAFE_HOST}_${DB_PORT}_${SAFE_USER}"
  fi
  SOURCE_DUMP_DIR="$DUMP_DIR/$SOURCE_KEY"
  SOURCE_LOG_DIR="$LOG_DIR/$SOURCE_KEY"
  mkdir -p "$SOURCE_DUMP_DIR" "$SOURCE_LOG_DIR"

  BACKUP_FILE="$SOURCE_DUMP_DIR/${DB_NAME}_${TIMESTAMP}.sql"
  COMPRESSED_FILE="${BACKUP_FILE}.gz"
  DB_LOG_FILE="$SOURCE_LOG_DIR/${DB_NAME}_${TIMESTAMP}.log"

  log "➡️ Backing up database: $DB_NAME ($DB_HOST:$DB_PORT)"

  IGNORE_TABLE_ARGS=()
  MYSQL_OPTION_ARGS=()
  CLEAN_MYSQL_OPTION_ARGS=()

  if [[ -n "${MYSQL_EXTRA_OPTIONS:-}" ]]; then
    IFS=',' read -ra MYSQL_OPTION_ARGS <<< "$MYSQL_EXTRA_OPTIONS"

    for option in "${MYSQL_OPTION_ARGS[@]}"; do
      option="$(echo "$option" | xargs)"
      [[ -z "$option" ]] && continue
      CLEAN_MYSQL_OPTION_ARGS+=("$option")
    done
  fi

  if [[ -n "${EXCLUDED_TABLES:-}" ]]; then
    IFS=',' read -ra TABLE_LIST <<< "$EXCLUDED_TABLES"

    for table in "${TABLE_LIST[@]}"; do
      # baştaki/sondaki boşlukları temizle
      table="$(echo "$table" | xargs)"
      [[ -z "$table" ]] && continue
      IGNORE_TABLE_ARGS+=("--ignore-table=$table")
    done
  fi

  if [[ ${#IGNORE_TABLE_ARGS[@]} -gt 0 ]]; then
    log "🚫 Excluding tables: ${EXCLUDED_TABLES}"
  fi

  DUMP_CMD=(
    mysqldump
    --host="$DB_HOST"
    --port="$DB_PORT"
    --user="$DB_USER"
    --single-transaction
    --quick
    --routines
    --triggers
    --events
  )

  if [[ ${#CLEAN_MYSQL_OPTION_ARGS[@]} -gt 0 ]]; then
    DUMP_CMD+=("${CLEAN_MYSQL_OPTION_ARGS[@]}")
  fi

  if [[ ${#IGNORE_TABLE_ARGS[@]} -gt 0 ]]; then
    DUMP_CMD+=("${IGNORE_TABLE_ARGS[@]}")
  fi
  DUMP_CMD+=("$DB_NAME")

  if MYSQL_PWD="$DB_PASS" "${DUMP_CMD[@]}" 2>"$DB_LOG_FILE" | gzip -c > "$COMPRESSED_FILE"; then
    if [[ ! -s "$DB_LOG_FILE" ]]; then
      rm -f "$DB_LOG_FILE"
    else
      log "⚠️ Dump warnings for $DB_NAME: $DB_LOG_FILE"
    fi
    log "✅ Backup success: $COMPRESSED_FILE"
    log "--------------------------------------------------"
    return 0
  fi

  log "❌ Backup failed: $DB_NAME"
  log "🧾 Dump error file: $DB_LOG_FILE"
  console "❌ Backup failed: $DB_NAME (details: $DB_LOG_FILE)"
  rm -f "$BACKUP_FILE" "$COMPRESSED_FILE"
  log "--------------------------------------------------"
  return 1
}

if ! acquire_lock; then
  exit 1
fi

log "📦 Backup process started"
log "📁 Backup directory: $BACKUP_DIR"
log "📂 Dump directory: $DUMP_DIR"
log "🧾 Log directory: $LOG_DIR"
log "📝 Run log file: $LOG_FILE"
log "⚙️ Parallel jobs: $MAX_PARALLEL_JOBS"

console "📦 Backup process started"
console "📂 Dump directory: $DUMP_DIR"
console "🧾 Run log file: $LOG_FILE"
console "⚙️ Parallel jobs: $MAX_PARALLEL_JOBS"

RESOLVED_DATABASES=()
for entry in "${DATABASES[@]}"; do
  if resolved_entries="$(resolve_database_entries "$entry")"; then
    while IFS= read -r resolved_entry; do
      [[ -z "$resolved_entry" ]] && continue
      RESOLVED_DATABASES+=("$resolved_entry")
    done <<< "$resolved_entries"
  else
    ((FAIL_COUNT++))
  fi
done

if [[ ${#RESOLVED_DATABASES[@]} -eq 0 ]]; then
  log "❌ No databases to backup after resolving DATABASES entries"
  log "✅ Successful: $SUCCESS_COUNT"
  log "❌ Failed: $FAIL_COUNT"
  console "❌ No databases to backup after resolving DATABASES entries"
  console "🧾 Run log file: $LOG_FILE"
  exit 1
fi

log "🗂️ Total databases queued for backup: ${#RESOLVED_DATABASES[@]}"
console "🗂️ Total databases queued for backup: ${#RESOLVED_DATABASES[@]}"

for entry in "${RESOLVED_DATABASES[@]}"; do
  while [[ "$(jobs -rp | wc -l | tr -d '[:space:]')" -ge "$MAX_PARALLEL_JOBS" ]]; do
    sleep 0.2
  done

  backup_database "$entry" &
  PIDS+=("$!")
done

COMPLETED_COUNT=0
for pid in "${PIDS[@]}"; do
  if wait "$pid"; then
    ((SUCCESS_COUNT++))
  else
    ((FAIL_COUNT++))
  fi
  ((COMPLETED_COUNT++))

  if (( COMPLETED_COUNT % CONSOLE_PROGRESS_EVERY == 0 || COMPLETED_COUNT == ${#PIDS[@]} )); then
    console "⏳ Progress: $COMPLETED_COUNT/${#PIDS[@]} | ✅ $SUCCESS_COUNT | ❌ $FAIL_COUNT"
  fi
done

log "🧹 Cleaning backups older than $RETENTION_DAYS days..."
find "$DUMP_DIR" -type f -name "*.sql.gz" -mtime +"$RETENTION_DAYS" -delete
find "$LOG_DIR" -type f -name "*.log" -mtime +"$RETENTION_DAYS" -delete
# Eski sürümden kalan kök log dosyalarını da temizle.
find "$BACKUP_DIR" -maxdepth 1 -type f -name "backup_*.log" -mtime +"$RETENTION_DAYS" -delete

log "🎉 Backup completed"
log "✅ Successful: $SUCCESS_COUNT"
log "❌ Failed: $FAIL_COUNT"
console "🎉 Backup completed | ✅ Successful: $SUCCESS_COUNT | ❌ Failed: $FAIL_COUNT"
console "🧾 Run log file: $LOG_FILE"
