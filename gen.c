#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void printUsage (const char* name) {
    printf("Usage:\n\t%s n [out.csv]\n\tGenerate n records and store in out.csv or write to stdout\n", name);
}

void random_name (char * out);
void random_date (char * out);

int main (int argc, char * argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        exit(-1);
    }

    int record_count = atoi(argv[1]);

    FILE *stream = stdout;

    if (argc > 2) {
        stream = fopen(argv[2], "w");

        if (!stream) {
            fprintf(stderr, "Couldn't open '%s'\n", argv[2]);
        }
    }

    srand(time(NULL));

    fprintf(stream, "id,name,birth_date,score\n");

    int prev_id = 100;

    for (int i = 0; i < record_count; i++) {
        int id = prev_id + rand() % 10 + 1;
        prev_id = id;
        char name[30];
        random_name(name);
        int score = rand() % 100;
        char birth_date[11];
        random_date(birth_date);

        fprintf(stream, "%d,%s,%s,%d\n", id, name, birth_date, score);
    }
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

int month_lengths[] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
void random_date (char *out) {
    int year = 1900 + rand() % 150;
    int month = rand() % 12 + 1;
    int month_length = month_lengths[month - 1];
    int day = rand() % (month_length) + 1;
    sprintf(out, "%04d-%02d-%02d", year, month, day);
}