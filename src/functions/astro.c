#include <math.h>
#include <stdio.h>
#include <string.h>

#include "astro.h"
#include "date.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

static const double DEG = M_PI / 180.0;
static const double RAD = 180.0 / M_PI;
static const double SYNODIC = 29.530588853;

static double norm360 (double x) {
    x = fmod(x, 360.0);
    if (x < 0) {
        x += 360.0;
    }
    return x;
}

static double norm1 (double x) {
    x = fmod(x, 1.0);
    if (x < 0) {
        x += 1.0;
    }
    return x;
}

double astroJulianToJD (int julian) {
    /* csvdb julian is truncated JD at civil midnight; true JD ≈ +0.5 */
    return (double)julian + 0.5;
}

void astroComputeDay (int julian, struct AstroDay *out) {
    double jd = astroJulianToJD(julian);
    double T = (jd - 2451545.0) / 36525.0;

    /* Sun (Meeus ch.25 simplified) */
    double L0 = norm360(
        280.46646 + 36000.76983 * T + 0.0003032 * T * T
    );
    double M = norm360(
        357.52911 + 35999.05029 * T - 0.0001537 * T * T
    );
    double Mr = M * DEG;
    double C =
        (1.914602 - 0.004817 * T - 0.000014 * T * T) * sin(Mr) +
        (0.019993 - 0.000101 * T) * sin(2.0 * Mr) +
        0.000289 * sin(3.0 * Mr);
    double true_long = norm360(L0 + C);
    double omega = norm360(125.04 - 1934.136 * T);
    double lambda = true_long - 0.00569 - 0.00478 * sin(omega * DEG);

    double epsilon0 =
        23.0 + (26.0 + (21.448 - T * (46.8150 + T * (0.00059 - T * 0.001813))) / 60.0) / 60.0;
    double epsilon = epsilon0 + 0.00256 * cos(omega * DEG);

    double sin_eps = sin(epsilon * DEG);
    double cos_eps = cos(epsilon * DEG);
    double sin_l = sin(lambda * DEG);
    double cos_l = cos(lambda * DEG);

    double decl = asin(sin_eps * sin_l) * RAD;
    double ra = atan2(cos_eps * sin_l, cos_l) * RAD;
    if (ra < 0) {
        ra += 360.0;
    }

    /* Equation of time (minutes): 4*(L0 - alpha) with alpha≈ra, L0 mean */
    double eot = 4.0 * (L0 - 0.0057183 - ra);
    if (eot > 180.0 * 4.0) {
        eot -= 1440.0;
    }
    if (eot < -180.0 * 4.0) {
        eot += 1440.0;
    }

    /* Moon mean elements (Meeus ch.47 low precision) */
    double Lp = norm360(
        218.3164477 + 481267.88123421 * T
        - 0.0015786 * T * T
    );
    double D = norm360(
        297.8501921 + 445267.1114034 * T
        - 0.0018819 * T * T
    );
    double Mmoon = norm360(
        134.9633964 + 477198.8675055 * T
        + 0.0087414 * T * T
    );
    double F = norm360(
        93.2720950 + 483202.0175233 * T
        - 0.0036539 * T * T
    );
    double Mm = M; /* sun mean anomaly already */

    double lunar_long = norm360(
        Lp
        + 6.289 * sin(Mmoon * DEG)
        + 1.274 * sin((2.0 * D - Mmoon) * DEG)
        + 0.658 * sin(2.0 * D * DEG)
        + 0.214 * sin(2.0 * Mmoon * DEG)
        - 0.186 * sin(Mm * DEG)
        - 0.114 * sin(2.0 * F * DEG)
    );

    /* Phase from elongation D (mean); illumination from phase angle */
    double phase = norm1(D / 360.0);
    double i = 180.0 - D
        - 6.289 * sin(Mmoon * DEG)
        + 2.100 * sin(Mm * DEG)
        - 1.274 * sin((2.0 * D - Mmoon) * DEG)
        - 0.658 * sin(2.0 * D * DEG);
    double illum = (1.0 + cos(i * DEG)) / 2.0;
    if (illum < 0) {
        illum = 0;
    }
    if (illum > 1) {
        illum = 1;
    }

    out->solar_longitude = lambda;
    out->equation_of_time = eot;
    out->solar_declination = decl;
    out->sun_ra = ra;
    out->lunar_longitude = lunar_long;
    out->moon_phase = phase;
    out->moon_illumination = illum;
    out->moon_age_days = phase * SYNODIC;
}

int astroPhaseCrossed (double prev_phase, double curr_phase, double target) {
    prev_phase = norm1(prev_phase);
    curr_phase = norm1(curr_phase);
    target = norm1(target);

    if (curr_phase >= prev_phase) {
        return prev_phase <= target && target < curr_phase;
    }
    /* wrapped past 1.0 */
    return prev_phase <= target || target < curr_phase;
}

int astroRiseSet (
    int julian,
    double lat_deg,
    double lon_deg,
    double decl_deg,
    double ra_deg,
    double h0_deg,
    int want_rise,
    struct DateTime *out
) {
    double lat = lat_deg * DEG;
    double decl = decl_deg * DEG;
    double h0 = h0_deg * DEG;

    double cos_H =
        (sin(h0) - sin(lat) * sin(decl)) / (cos(lat) * cos(decl));
    if (cos_H < -1.0 || cos_H > 1.0) {
        return 0;
    }

    double H = acos(cos_H) * RAD; /* 0..180 */

    /* Approximate Greenwich sidereal at 0h UT for JD midnight+ */
    double jd0 = (double)julian + 0.5; /* ~UT midnight of civil day */
    double T = (jd0 - 2451545.0) / 36525.0;
    double theta0 = norm360(
        100.46061837 + 36000.770053608 * T + 0.000387933 * T * T
    );

    /* Local mean solar-ish transit: when LST = RA */
    double transit = norm1((ra_deg + lon_deg - theta0) / 360.0);
    double event;
    if (want_rise) {
        event = norm1(transit - H / 360.0);
    } else {
        event = norm1(transit + H / 360.0);
    }

    int minutes = (int)(event * 24.0 * 60.0 + 0.5);
    if (minutes >= 24 * 60) {
        minutes -= 24 * 60;
    }

    datetimeFromJulian(out, julian);
    out->hour = minutes / 60;
    out->minute = minutes % 60;
    out->second = 0;
    out->tz_offset_minutes = TZ_LOCAL;
    return 1;
}

void astroEclipseFlags (int julian, int *lunar, int *solar) {
    struct AstroDay d;
    astroComputeDay(julian, &d);

    /* Node proximity via F (argument of latitude) — recompute F */
    double jd = astroJulianToJD(julian);
    double T = (jd - 2451545.0) / 36525.0;
    double F = norm360(
        93.2720950 + 483202.0175233 * T - 0.0036539 * T * T
    );
    /* Distance to nearest node (0 or 180) */
    double f_mod = fmod(F, 180.0);
    if (f_mod < 0) {
        f_mod += 180.0;
    }
    double node_dist = f_mod > 90.0 ? 180.0 - f_mod : f_mod;

    int near_node = node_dist < 12.0; /* generous season window */

    *solar = 0;
    *lunar = 0;
    if (!near_node) {
        return;
    }

    /* New moon + near node → solar; full + near node → lunar */
    if (d.moon_phase < 0.03 || d.moon_phase > 0.97) {
        *solar = 1;
    }
    if (d.moon_phase > 0.47 && d.moon_phase < 0.53) {
        *lunar = 1;
    }

    /* Also flag if phase crossed today */
    struct AstroDay prev;
    astroComputeDay(julian - 1, &prev);
    if (near_node && astroPhaseCrossed(prev.moon_phase, d.moon_phase, 0.0)) {
        *solar = 1;
    }
    if (near_node && astroPhaseCrossed(prev.moon_phase, d.moon_phase, 0.5)) {
        *lunar = 1;
    }
}
