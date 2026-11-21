#include "../include/ud/scheduler.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

#include <unicode/ucal.h>
#include <unicode/ustring.h>

/* The calendar is intentionally independent of the managed runtime.  ICU is
 * the single timezone database used by Native; this avoids the host's local
 * timezone and keeps DST behavior identical for every binding. */

static int id_valid(ud_string value) {
    if (!value.data || !value.length || value.length > 128 || memchr(value.data, 0, value.length))
        return 0;
    if (value.length == 1 && value.data[0] == '.')
        return 0;
    if (value.length == 2 && value.data[0] == '.' && value.data[1] == '.')
        return 0;
    for (uint32_t at = 0; at < value.length; at++) {
        char c = value.data[at];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.'))
            return 0;
    }
    return 1;
}

static int string_valid(ud_string value) {
    return (!value.length || (value.data && !memchr(value.data, 0, value.length)));
}

static int64_t max_unix_ms(void) {
    /* DateTimeOffset.MaxValue (9999-12-31T23:59:59.999Z).  Keeping this
     * bound here makes the native result exactly representable by the public
     * .NET DateTimeOffset used by the management API. */
    return INT64_C(253402300799999);
}

static ud_status check_shape(const ud_scheduler_schedule *schedule) {
    if (!schedule)
        return UD_INVALID;
    if (schedule->struct_size != sizeof(*schedule) || schedule->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!id_valid(schedule->id) || !string_valid(schedule->time_zone) || schedule->time_zone.length > 255 ||
        schedule->start_ms < 0 || schedule->start_ms > max_unix_ms() ||
        (schedule->enabled != 0 && schedule->enabled != 1) ||
        (schedule->has_local_time != 0 && schedule->has_local_time != 1) ||
        schedule->frequency < UD_SCHEDULE_ONCE || schedule->frequency > UD_SCHEDULE_MONTHLY ||
        schedule->local_ticks >= UINT64_C(864000000000))
        return UD_INVALID;
    if (schedule->frequency == UD_SCHEDULE_INTERVAL && !schedule->interval_seconds)
        return UD_INVALID;
    if (schedule->frequency == UD_SCHEDULE_WEEKLY &&
        (!schedule->weekdays_mask || schedule->weekdays_count == 0 || schedule->weekdays_count > 7 ||
         (schedule->weekdays_mask & ~UINT32_C(0x7f))))
        return UD_INVALID;
    if (schedule->frequency == UD_SCHEDULE_MONTHLY &&
        (schedule->day_of_month < 1 || schedule->day_of_month > 31))
        return UD_INVALID;
    if (schedule->frequency >= UD_SCHEDULE_DAILY && (!schedule->time_zone.length || !schedule->has_local_time))
        return UD_INVALID;
    if (schedule->frequency >= UD_SCHEDULE_DAILY &&
        (schedule->local_ticks >= UINT64_C(864000000000)))
        return UD_INVALID;
    return UD_OK;
}

static UCalendar *open_zone(ud_string zone, UErrorCode *error) {
    /* An ICU timezone ID is short in practice; the explicit bound also keeps
     * this ABI operation allocation-free and rejects a malformed giant ID. */
    if (!zone.length || zone.length > 255)
        return NULL;
    UChar name[256];
    int32_t units = 0;
    u_strFromUTF8(name, (int32_t)(sizeof(name) / sizeof(name[0])), &units,
                  zone.data, (int32_t)zone.length, error);
    if (U_FAILURE(*error))
        return NULL;
    UChar canonical[256];
    UBool system_zone = 0;
    int32_t canonical_units = ucal_getCanonicalTimeZoneID(
        name, units, canonical, (int32_t)(sizeof(canonical) / sizeof(canonical[0])),
        &system_zone, error);
    if (canonical_units <= 0 || U_FAILURE(*error) || !system_zone)
        return NULL;
    UCalendar *calendar = ucal_open(name, units, "en_US", UCAL_GREGORIAN, error);
    if (!calendar || U_FAILURE(*error)) {
        if (calendar)
            ucal_close(calendar);
        return NULL;
    }
    /* A repeated wall time chooses its first UTC instant.  A skipped wall
     * time is detected below with lenient=false and is not shifted forward. */
    ucal_setAttribute(calendar, UCAL_LENIENT, 0);
    ucal_setAttribute(calendar, UCAL_REPEATED_WALL_TIME, UCAL_WALLTIME_FIRST);
    ucal_setAttribute(calendar, UCAL_SKIPPED_WALL_TIME, UCAL_WALLTIME_FIRST);
    return calendar;
}

static ud_status check_zone(const ud_scheduler_schedule *schedule) {
    if (schedule->frequency < UD_SCHEDULE_DAILY)
        return UD_OK;
    UErrorCode error = U_ZERO_ERROR;
    UCalendar *calendar = open_zone(schedule->time_zone, &error);
    if (!calendar || U_FAILURE(error))
        return UD_INVALID;
    ucal_close(calendar);
    return UD_OK;
}

ud_status ud_scheduler_validate(const ud_scheduler_schedule *schedule) {
    ud_status status = check_shape(schedule);
    if (!status)
        status = check_zone(schedule);
    return status;
}

static int local_millis(const ud_scheduler_schedule *schedule, int32_t *millis) {
    uint64_t value = schedule->local_ticks / UINT64_C(10000);
    if (value >= UINT64_C(86400000))
        return 0;
    *millis = (int32_t)value;
    return 1;
}

static int64_t interval_next(const ud_scheduler_schedule *schedule, int64_t after, ud_status *status) {
    if (schedule->start_ms > after)
        return schedule->start_ms;
    uint64_t period = (uint64_t)schedule->interval_seconds * UINT64_C(1000);
    uint64_t elapsed = (uint64_t)(after - schedule->start_ms);
    uint64_t steps = elapsed / period + UINT64_C(1);
    if (steps > ((uint64_t)max_unix_ms() - (uint64_t)schedule->start_ms) / period) {
        *status = UD_END;
        return 0;
    }
    return schedule->start_ms + (int64_t)(steps * period);
}

static int calendar_date(const UCalendar *calendar, int32_t *year, int32_t *month, int32_t *day,
                         UErrorCode *error) {
    *year = ucal_get(calendar, UCAL_YEAR, error);
    *month = ucal_get(calendar, UCAL_MONTH, error);
    *day = ucal_get(calendar, UCAL_DATE, error);
    return U_SUCCESS(*error);
}

static int weekday_matches(const ud_scheduler_schedule *schedule, const UCalendar *calendar,
                           UErrorCode *error) {
    if (schedule->frequency != UD_SCHEDULE_WEEKLY)
        return 1;
    int32_t weekday = ucal_get(calendar, UCAL_DAY_OF_WEEK, error);
    return U_SUCCESS(*error) && weekday >= UCAL_SUNDAY && weekday <= UCAL_SATURDAY &&
           (schedule->weekdays_mask & (UINT32_C(1) << (weekday - UCAL_SUNDAY))) != 0;
}

static int candidate(const ud_scheduler_schedule *schedule, UCalendar *calendar, int32_t year,
                     int32_t month, int32_t day, int32_t local_ms, int64_t after, int64_t *result) {
    if (schedule->frequency == UD_SCHEDULE_MONTHLY && day != schedule->day_of_month)
        return 0;
    UErrorCode error = U_ZERO_ERROR;
    int32_t wall_hour = local_ms / 3600000;
    int32_t wall_minute = (local_ms % 3600000) / 60000;
    int32_t wall_second = (local_ms % 60000) / 1000;
    int32_t wall_millisecond = local_ms % 1000;
    ucal_clear(calendar);
    ucal_setAttribute(calendar, UCAL_LENIENT, 0);
    ucal_setAttribute(calendar, UCAL_REPEATED_WALL_TIME, UCAL_WALLTIME_FIRST);
    ucal_setAttribute(calendar, UCAL_SKIPPED_WALL_TIME, UCAL_WALLTIME_FIRST);
    ucal_set(calendar, UCAL_YEAR, year);
    ucal_set(calendar, UCAL_MONTH, month);
    ucal_set(calendar, UCAL_DATE, day);
    ucal_set(calendar, UCAL_HOUR_OF_DAY, wall_hour);
    ucal_set(calendar, UCAL_MINUTE, wall_minute);
    ucal_set(calendar, UCAL_SECOND, wall_second);
    ucal_set(calendar, UCAL_MILLISECOND, wall_millisecond);
    UDate value = ucal_getMillis(calendar, &error);
    /* ICU reports U_ILLEGAL_ARGUMENT_ERROR for a local time in a spring
     * forward gap when lenient mode is disabled.  It is intentionally skipped
     * rather than silently moved to the next valid wall time. */
    if (U_FAILURE(error) || value < 0 || value > (UDate)max_unix_ms())
        return 0;
    /* ICU's skipped-wall-time policy may choose a neighboring valid instant
     * instead of returning an error.  Round-trip the instant through the
     * calendar so a spring-forward gap is rejected rather than silently
     * shifted.  The same round-trip preserves the explicitly selected first
     * instant when a fall-back wall time occurs twice. */
    ucal_setMillis(calendar, value, &error);
    int32_t actual_year = ucal_get(calendar, UCAL_YEAR, &error);
    int32_t actual_month = ucal_get(calendar, UCAL_MONTH, &error);
    int32_t actual_day = ucal_get(calendar, UCAL_DATE, &error);
    int32_t actual_hour = ucal_get(calendar, UCAL_HOUR_OF_DAY, &error);
    int32_t actual_minute = ucal_get(calendar, UCAL_MINUTE, &error);
    int32_t actual_second = ucal_get(calendar, UCAL_SECOND, &error);
    int32_t actual_millisecond = ucal_get(calendar, UCAL_MILLISECOND, &error);
    if (U_FAILURE(error) || actual_year != year || actual_month != month || actual_day != day ||
        actual_hour != wall_hour || actual_minute != wall_minute ||
        actual_second != wall_second || actual_millisecond != wall_millisecond)
        return 0;
    int64_t instant = (int64_t)value;
    if (instant < schedule->start_ms || instant <= after)
        return 0;
    *result = instant;
    return 1;
}

static ud_status calendar_next(const ud_scheduler_schedule *schedule, int64_t after, int64_t *result) {
    UErrorCode error = U_ZERO_ERROR;
    UCalendar *calendar = open_zone(schedule->time_zone, &error);
    if (!calendar || U_FAILURE(error)) {
        if (calendar)
            ucal_close(calendar);
        return UD_INVALID;
    }
    int32_t local_ms = 0;
    if (!local_millis(schedule, &local_ms)) {
        ucal_close(calendar);
        return UD_INVALID;
    }
    int64_t lower = after < schedule->start_ms ? schedule->start_ms - 1 : after;
    ucal_setMillis(calendar, (UDate)lower, &error);
    int32_t year = 0, month = 0, day = 0;
    if (!calendar_date(calendar, &year, &month, &day, &error)) {
        ucal_close(calendar);
        return UD_END;
    }
    /* The local date may be at most one day before the first valid local
     * occurrence when an offset transition is involved; starting today and
     * advancing one day at a time keeps the strict-after rule explicit. */
    /* From Unix epoch through DateTimeOffset.MaxValue is just under three
     * million Gregorian dates; retain a bounded loop while covering that
     * complete public range. */
    for (uint32_t attempts = 0; attempts < 4000000; attempts++) {
        error = U_ZERO_ERROR;
        int match = schedule->frequency != UD_SCHEDULE_WEEKLY || weekday_matches(schedule, calendar, &error);
        if (!U_SUCCESS(error)) {
            ucal_close(calendar);
            return UD_END;
        }
        if (match && (schedule->frequency != UD_SCHEDULE_MONTHLY || day == schedule->day_of_month) &&
            candidate(schedule, calendar, year, month, day, local_ms, after, result)) {
            ucal_close(calendar);
            return UD_OK;
        }
        /* Advance the date using the same Gregorian calendar, then read the
         * resulting date fields.  The time-of-day fields are irrelevant here;
         * candidate() clears them before conversion. */
        error = U_ZERO_ERROR;
        ucal_add(calendar, UCAL_DATE, 1, &error);
        if (!U_SUCCESS(error) || !calendar_date(calendar, &year, &month, &day, &error))
            break;
        if (year > 9999) /* no DateTimeOffset value can represent a later date */
            break;
    }
    ucal_close(calendar);
    return UD_END;
}

ud_status ud_scheduler_next(const ud_scheduler_schedule *schedule, int64_t after_ms, int64_t *next_ms) {
    if (!next_ms)
        return UD_INVALID;
    *next_ms = 0;
    ud_status status = ud_scheduler_validate(schedule);
    if (status)
        return status;
    if (schedule->frequency == UD_SCHEDULE_ONCE) {
        if (schedule->start_ms > after_ms) {
            *next_ms = schedule->start_ms;
            return UD_OK;
        }
        return UD_END;
    }
    if (schedule->frequency == UD_SCHEDULE_INTERVAL) {
        *next_ms = interval_next(schedule, after_ms, &status);
        return status;
    }
    return calendar_next(schedule, after_ms, next_ms);
}
