#!/usr/bin/env bash
set -euo pipefail
APP=/root/teflow
TARGET=/usr/local/bin/tf
chmod 755 "$APP/tf.sh"
if [[ -e "$TARGET" || -L "$TARGET" ]]; then
    if [[ "$(readlink -f "$TARGET")" != "$APP/tf.sh" ]]; then
        printf 'tf already exists at %s; keeping it unchanged.\n' "$TARGET" >&2
        exit 1
    fi
elif [[ -w /usr/local/bin ]]; then
    ln -s "$APP/tf.sh" "$TARGET"
else
    sudo ln -s "$APP/tf.sh" "$TARGET"
fi
printf 'tf installed: %s -> %s/tf.sh\n' "$TARGET" "$APP"
