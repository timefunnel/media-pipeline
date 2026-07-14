#!/bin/sh
set -eu

alias_host="${PANSOU_TME_ALIAS_HOST:-telegram.me}"
target_host="t.me"
refresh_seconds="${PANSOU_TME_REFRESH_SECONDS:-3600}"
hosts_file="/etc/hosts"
refresh_stamp="/tmp/pansou-tme-last-refresh"

case "$refresh_seconds" in
    ""|*[!0-9]*)
        echo "PANSOU_TME_REFRESH_SECONDS must be a positive integer" >&2
        exit 1
        ;;
esac
if [ "$refresh_seconds" -le 0 ]; then
    echo "PANSOU_TME_REFRESH_SECONDS must be greater than zero" >&2
    exit 1
fi

resolve_alias_ipv4() {
    busybox nslookup "$alias_host" | awk -v alias="$alias_host" '
        $1 == "Name:" {
            name = $2
            sub(/\.$/, "", name)
            found = (name == alias)
            next
        }
        found && $1 == "Address:" && $2 ~ /^[0-9]+\./ {
            print $2
            exit
        }
    '
}

refresh_tme_mapping() {
    ip="$(resolve_alias_ipv4)"
    case "$ip" in
        ""|*[!0-9.]*)
            echo "invalid IPv4 address resolved for $alias_host: $ip" >&2
            return 1
            ;;
    esac

    temp_hosts="$(mktemp)"
    awk -v target="$target_host" '
        {
            keep = 1
            for (i = 2; i <= NF; i++) {
                if ($i == target) {
                    keep = 0
                }
            }
            if (keep) {
                print
            }
        }
    ' "$hosts_file" >"$temp_hosts"
    printf '%s %s\n' "$ip" "$target_host" >>"$temp_hosts"
    cat "$temp_hosts" >"$hosts_file"
    rm -f "$temp_hosts"

    probe_channel="${CHANNELS%%,*}"
    case "$probe_channel" in
        ""|*[!A-Za-z0-9_]*)
            echo "cannot derive a valid Telegram probe channel from CHANNELS" >&2
            return 1
            ;;
    esac
    wget -q -O /dev/null -T 10 "https://$target_host/s/$probe_channel"
    date +%s >"$refresh_stamp"
    printf '%s resolved through %s: %s\n' "$target_host" "$alias_host" "$ip"
}

mapping_refresh_is_due() {
    if [ ! -f "$refresh_stamp" ]; then
        return 0
    fi

    last_refresh="$(cat "$refresh_stamp" 2>/dev/null || printf '0')"
    case "$last_refresh" in
        ""|*[!0-9]*) last_refresh=0 ;;
    esac
    now="$(date +%s)"
    [ "$((now - last_refresh))" -ge "$refresh_seconds" ]
}

case "${1:-}" in
    --healthcheck)
        if mapping_refresh_is_due; then
            refresh_tme_mapping
        fi
        exec wget -q --spider -T 5 http://127.0.0.1:8888/api/health
        ;;
    --refresh-only)
        refresh_tme_mapping
        ;;
    "")
        refresh_tme_mapping
        exec /app/pansou
        ;;
    *)
        echo "unsupported argument: $1" >&2
        exit 2
        ;;
esac
