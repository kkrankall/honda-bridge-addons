#!/usr/bin/with-contenv bashio
set -e

export HONDA_EMAIL="$(bashio::config 'honda_email')"
export HONDA_PASSWORD="$(bashio::config 'honda_password')"
export HONDA_PIN="$(bashio::config 'honda_pin')"
export VIN="$(bashio::config 'vin')"
export POLL_INTERVAL_SECONDS="$(bashio::config 'poll_interval_seconds')"
export ENABLE_DAY_NIGHT_SCHEDULE="$(bashio::config 'enable_day_night_schedule')"
export POLL_INTERVAL_DAY="$(bashio::config 'poll_interval_day')"
export POLL_INTERVAL_NIGHT="$(bashio::config 'poll_interval_night')"
export LATITUDE="$(bashio::config 'latitude')"
export LONGITUDE="$(bashio::config 'longitude')"
export DAY_START_HOUR="$(bashio::config 'day_start_hour')"
export DAY_END_HOUR="$(bashio::config 'day_end_hour')"
export MQTT_HOST="$(bashio::config 'mqtt_host')"
export MQTT_PORT="$(bashio::config 'mqtt_port')"
export MQTT_USER="$(bashio::config 'mqtt_user')"
export MQTT_PASSWORD="$(bashio::config 'mqtt_password')"
export DEVICE_NAME="$(bashio::config 'device_name')"
export SUMMER_EFFICIENCY_MI_PER_KWH="$(bashio::config 'summer_efficiency_mi_per_kwh')"
export WINTER_EFFICIENCY_MI_PER_KWH="$(bashio::config 'winter_efficiency_mi_per_kwh')"
export SUMMER_MONTHS="$(bashio::config 'summer_months')"
export LOG_LEVEL="$(bashio::config 'log_level')"
export STATE_DIR="/data"

bashio::log.info "Starting HondaLink Bridge for VIN ${VIN}"
exec python3 /honda_bridge.py
