#pragma once

#include "../structs.h"

/**
 * Compact Meeus-style solar/lunar quantities for a civil julian day index
 * (csvdb midnight julian from datetimeGetJulian).
 */
struct AstroDay {
    double solar_longitude;   /* degrees 0..360 */
    double equation_of_time;  /* minutes */
    double solar_declination; /* degrees */
    double lunar_longitude;   /* degrees 0..360 */
    double moon_phase;        /* 0 = new … 0.5 = full … 1 = next new */
    double moon_illumination; /* 0..1 */
    double moon_age_days;     /* 0 .. ~29.53 */
    double sun_ra;            /* degrees, for rise/set */
    double moon_ra;           /* degrees, for rise/set */
    double moon_decl;         /* degrees, for rise/set */
};

/** Fill AstroDay for noon on the civil day identified by julian. */
void astroComputeDay (int julian, struct AstroDay *out);

/**
 * Phase-crossing flags vs previous civil day (1 if that phase occurs on this
 * day). Targets: new=0, first=0.25, full=0.5, last=0.75.
 */
int astroPhaseCrossed (double prev_phase, double curr_phase, double target);

/** Approximate true JD (noon) from csvdb civil julian index. */
double astroJulianToJD (int julian);

/**
 * Rise/set local civil times as untagged DateTime on `julian`'s date.
 * Returns 1 if event exists, 0 if always above/below (polar).
 * h0_deg: standard altitude (sun ≈ -0.833, moon ≈ 0.125).
 */
int astroRiseSet (
    int julian,
    double lat_deg,
    double lon_deg,
    double decl_deg,
    double ra_deg,
    double h0_deg,
    int want_rise,
    struct DateTime *out
);

/**
 * Lunar/solar eclipse proximity for this civil day (approximate).
 * Sets *lunar / *solar to 1 when geometry suggests an eclipse near this day.
 */
void astroEclipseFlags (int julian, int *lunar, int *solar);
