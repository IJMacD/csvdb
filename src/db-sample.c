#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "date.h"
#include "db.h"
#include "db-sample.h"
#include "predicates.h"
#include "result.h"

#define LCG_A   15961   // Chosen by fair dice roll - must be odd (if M is power of 2)
#define LCG_C   13281   // Chosen by fair dice roll - must be 4K + 1
#define LCG_M   (1<<17)   // will give range of (2 * 65536) days (approx 360 years)

#define BASE_DATE   2341972 // 1700-01-01

static char *field_names[] = {
    "id",
    "name",
    "birth_date",
    "score",
};

void random_name (char * out);
void random_date (char * out);
int lcg ();

int lcg_value;
int prev_id;

int sample_openDB (struct DB *db, const char *filename) {
    if (strcmp(filename, "SAMPLE") != 0) {
        return -1;
    }

    db->vfs = VFS_SAMPLE;
    db->record_count = 1e8;
    db->line_indices = NULL;
    db->field_count = sizeof(field_names) / sizeof(field_names[0]);

    lcg_value = rand() % LCG_M;

    return 0;
}

void sample_closeDB (__attribute__((unused)) struct DB *db) {}

int sample_getFieldIndex (struct DB *db, const char *field) {
    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }

    return -1;
}

char *sample_getFieldName (__attribute__((unused)) struct DB *db, int field_index) {
    return field_names[field_index];
}

int sample_getRecordValue (__attribute__((unused)) struct DB *db, __attribute__((unused)) int record_index, int field_index, char *value, __attribute__((unused)) size_t value_max_length) {

    // id
    if (field_index == 0) {
        int id = prev_id + rand() % 10 + 1;
        prev_id = id;
        return sprintf(value, "%d", id);
    }

    // name
    if (field_index == 1) {
        random_name(value);
        return 1;
    }

    // birth_date
    if (field_index == 2) {
        random_date(value);
        return 10;
    }

    // score
    if (field_index == 3) {
        return sprintf(value, "%d", rand() % 100);
    }

    return -1;
}

// All queries go through fullTableScan
int sample_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return 0;
}

char *first_names[] = { "John","William","James","Charles","George","Frank","Joseph","Thomas","Henry","Robert","Edward","Harry","Walter","Arthur","Fred","Albert","Samuel","David","Louis","Joe","Charlie","Clarence","Richard","Andrew","Daniel","Ernest","Will","Jesse","Oscar","Lewis","Peter","Benjamin","Frederick","Willie","Alfred","Sam","Roy","Herbert","Jacob","Tom","Elmer","Carl","Lee","Howard","Martin","Michael","Bert","Herman","Jim","Francis","Harvey","Earl","Eugene","Ralph","Ed","Claude","Edwin","Ben","Charley","Paul","Edgar","Isaac","Otto","Luther","Lawrence","Ira","Patrick","Guy","Oliver","Theodore","Hugh","Clyde","Alexander","August","Floyd","Homer","Jack","Leonard","Horace","Marion","Philip","Allen","Archie","Stephen","Chester","Willis","Raymond","Rufus","Warren","Jessie","Milton","Alex","Leo","Julius","Ray","Sidney","Bernard","Dan","Jerry","Calvin","Perry","Dave","Anthony","Eddie","Amos","Dennis","Clifford","Leroy","Wesley","Alonzo","Garfield","Franklin","Emil","Leon","Nathan","Harold","Matthew","Levi","Moses","Everett","Lester","Winfield","Adam","Lloyd","Mack","Fredrick","Jay","Jess","Melvin","Noah","Aaron","Alvin","Norman","Gilbert","Elijah","Victor","Gus","Nelson","Jasper","Silas","Christopher","Jake","Mike","Percy","Adolph","Maurice","Cornelius","Felix","Reuben","Wallace","Claud","Roscoe","Sylvester","Earnest","Hiram","Otis","Simon","Willard","Irvin","Mark","Jose","Wilbur","Abraham","Virgil","Clinton","Elbert","Leslie","Marshall","Owen","Wiley","Anton","Morris","Manuel","Phillip","Augustus","Emmett","Eli","Nicholas","Wilson","Alva","Harley","Newton","Timothy","Marvin","Ross","Curtis","Edmund","Jeff","Elias","Harrison","Stanley","Columbus","Lon","Ora","Ollie","Russell","Pearl","Solomon","Arch","Asa","Clayton","Enoch","Irving","Mathew","Nathaniel" };
char *last_names[] = { "SMITH","JOHNSON","WILLIAMS","BROWN","JONES","MILLER","DAVIS","GARCIA","RODRIGUEZ","WILSON","MARTINEZ","ANDERSON","TAYLOR","THOMAS","HERNANDEZ","MOORE","MARTIN","JACKSON","THOMPSON","WHITE","LOPEZ","LEE","GONZALEZ","HARRIS","CLARK","LEWIS","ROBINSON","WALKER","PEREZ","HALL","YOUNG","ALLEN","SANCHEZ","WRIGHT","KING","SCOTT","GREEN","BAKER","ADAMS","NELSON","HILL","RAMIREZ","CAMPBELL","MITCHELL","ROBERTS","CARTER","PHILLIPS","EVANS","TURNER","TORRES","PARKER","COLLINS","EDWARDS","STEWART","FLORES","MORRIS","NGUYEN","MURPHY","RIVERA","COOK","ROGERS","MORGAN","PETERSON","COOPER","REED","BAILEY","BELL","GOMEZ","KELLY","HOWARD","WARD","COX","DIAZ","RICHARDSON","WOOD","WATSON","BROOKS","BENNETT","GRAY","JAMES","REYES","CRUZ","HUGHES","PRICE","MYERS","LONG","FOSTER","SANDERS","ROSS","MORALES","POWELL","SULLIVAN","RUSSELL","ORTIZ","JENKINS","GUTIERREZ","PERRY","BUTLER","BARNES","FISHER","HENDERSON","COLEMAN","SIMMONS","PATTERSON","JORDAN","REYNOLDS","HAMILTON","GRAHAM","KIM","GONZALES","ALEXANDER","RAMOS","WALLACE","GRIFFIN","WEST","COLE","HAYES","CHAVEZ","GIBSON","BRYANT","ELLIS","STEVENS","MURRAY","FORD","MARSHALL","OWENS","MCDONALD","HARRISON","RUIZ","KENNEDY","WELLS","ALVAREZ","WOODS","MENDOZA","CASTILLO","OLSON","WEBB","WASHINGTON","TUCKER","FREEMAN","BURNS","HENRY","VASQUEZ","SNYDER","SIMPSON","CRAWFORD","JIMENEZ","PORTER","MASON","SHAW","GORDON","WAGNER","HUNTER","ROMERO","HICKS","DIXON","HUNT","PALMER","ROBERTSON","BLACK","HOLMES","STONE","MEYER","BOYD","MILLS","WARREN","FOX","ROSE","RICE","MORENO","SCHMIDT","PATEL","FERGUSON","NICHOLS","HERRERA","MEDINA","RYAN","FERNANDEZ","WEAVER","DANIELS","STEPHENS","GARDNER","PAYNE","KELLEY","DUNN","PIERCE","ARNOLD","TRAN","SPENCER","PETERS","HAWKINS","GRANT","HANSEN","CASTRO","HOFFMAN","HART","ELLIOTT","CUNNINGHAM","KNIGHT","BRADLEY","CARROLL","HUDSON","DUNCAN","ARMSTRONG","BERRY","ANDREWS","JOHNSTON","RAY","LANE","RILEY","CARPENTER","PERKINS","AGUILAR","SILVA","RICHARDS","WILLIS","MATTHEWS","CHAPMAN","LAWRENCE","GARZA","VARGAS","WATKINS","WHEELER","LARSON","CARLSON","HARPER","GEORGE","GREENE","BURKE","GUZMAN","MORRISON","MUNOZ","JACOBS","OBRIEN","LAWSON","FRANKLIN","LYNCH","BISHOP","CARR","SALAZAR","AUSTIN","MENDEZ","GILBERT","JENSEN","WILLIAMSON","MONTGOMERY","HARVEY","OLIVER","HOWELL","DEAN","HANSON","WEBER","GARRETT","SIMS","BURTON","FULLER","SOTO","MCCOY","WELCH","CHEN","SCHULTZ","WALTERS","REID","FIELDS","WALSH","LITTLE","FOWLER","BOWMAN","DAVIDSON","MAY","DAY","SCHNEIDER","NEWMAN","BREWER","LUCAS","HOLLAND","WONG","BANKS","SANTOS","CURTIS","PEARSON","DELGADO","VALDEZ","PENA","RIOS","DOUGLAS","SANDOVAL","BARRETT","HOPKINS","KELLER","GUERRERO","STANLEY","BATES","ALVARADO","BECK","ORTEGA","WADE","ESTRADA","CONTRERAS","BARNETT","CALDWELL","SANTIAGO","LAMBERT","POWERS","CHAMBERS","NUNEZ","CRAIG","LEONARD","LOWE","RHODES","BYRD","GREGORY","SHELTON","FRAZIER","BECKER" };
int first_count = sizeof(first_names) / sizeof(first_names[0]);
int last_count = sizeof(last_names) / sizeof(last_names[0]);
void random_name (char *out) {
    int index_f = rand() % first_count;
    int index_l = rand() % last_count;
    sprintf(out, "%s %s", first_names[index_f], last_names[index_l]);
}

void random_date (char *out) {
    int julian = BASE_DATE + lcg();
    struct DateTime dt;
    datetimeFromJulian(&dt, julian);
    sprintf(out, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
}

int lcg () {
    lcg_value = (LCG_A * lcg_value + LCG_C) % LCG_M;
    return lcg_value;
}