import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES
immigration_staging_table_drop = "drop table if exists immigration_staging cascade"
immigration_table_drop = "drop table if exists immigration cascade"
ports_table_drop = "drop table if exists ports cascade"
countries_table_drop = "drop table if exists countries cascade"
modes_table_drop = "drop table if exists modes cascade"
states_table_drop = "drop table if exists states cascade"
visa_categories_table_drop = "drop table if exists visa_categories cascade"
visa_types_table_drop = "drop table if exists visa_types cascade"
airlines_table_drop = "drop table if exists airlines cascade"
demographics_table_drop = "drop table if exists demographics cascade"

"""
 |-- cicid: double (nullable = true)    drop column
 |-- i94yr: double (nullable = true)    int - all are 2016, remove nulls
 |-- i94mon: double (nullable = true)   varchar - all are from the month imported, remove nulls
 |-- i94cit: double (nullable = true)   int - country code, import description list from label desc file
 |-- i94res: double (nullable = true)   int - country code, import description list from label desc file
 |-- arrdate: double (nullable = true)  numeric - sas date field, convert to datetime
 |-- i94mode: double (nullable = true)  int - mode of arrival, build dim tbale from label desc file, very few nulls, remove nulls
 |-- i94addr: string (nullable = true)  varchar - state abbrev, import description list from label desc file, about 150K nulls, remove nulls and non matching values
 |-- depdate: double (nullable = true)  numeric - sas date field, convert to datetime, nearly 300k nulls
 |-- i94bir: double (nullable = true)   int - age field in years
 |-- i94visa: double (nullable = true)  int - visa code, build dim table from label desc file, no nulls
 |-- count: double (nullable = true)    drop column
 |-- dtadfile: string (nullable = true) drop column
 |-- visapost: string (nullable = true) varchar - dept of state where vis issued, 1.5M nulls, leave nulls
 |-- occup: string (nullable = true)    drop column, occupation, 2.5M nulls
 |-- entdepa: string (nullable = true)  varchar - can include but need info for dim table
 |-- entdepd: string (nullable = true)  varchar - can include but need info for dim table
 |-- entdepu: string (nullable = true)  drop column
 |-- matflag: string (nullable = true)  drop column
 |-- biryear: double (nullable = true)  int - birth year
 |-- dtaddto: string (nullable = true)  int - convert to date time, date admitted to us (allowed to stay till)
 |-- gender: string (nullable = true)   varchar - can include but need info for dim table
 |-- insnum: string (nullable = true)   drop column
 |-- airline: string (nullable = true)  varrchar - no way to translate, unless outside source data
 |-- admnum: double (nullable = true)   drop column
 |-- fltno: string (nullable = true)    int - flight number
 |-- visatype: string (nullable = true) varchar - no way to translate, unless outside source data
 |-- i94port: string (nullable = true)  varchar - airport code, import description list from label desc file

 |-- cicid: double (nullable = true)
 |-- i94yr: double (nullable = true)
 |-- i94mon: double (nullable = true)
 |-- i94cit: double (nullable = true)
 |-- i94res: double (nullable = true)
 |-- i94port: string (nullable = true)
 |-- arrdate: double (nullable = true)
 |-- i94mode: double (nullable = true)
 |-- i94addr: string (nullable = true)
 |-- depdate: double (nullable = true)
 |-- i94bir: double (nullable = true)
 |-- i94visa: double (nullable = true)
 |-- count: double (nullable = true)
 |-- dtadfile: string (nullable = true)
 |-- visapost: string (nullable = true)
 |-- occup: string (nullable = true)
 |-- entdepa: string (nullable = true)
 |-- entdepd: string (nullable = true)
 |-- entdepu: string (nullable = true)
 |-- matflag: string (nullable = true)
 |-- biryear: double (nullable = true)
 |-- dtaddto: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- insnum: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- admnum: double (nullable = true)
 |-- fltno: string (nullable = true)
 |-- visatype: string (nullable = true)
"""

immigration_staging_table_create = ("""
    create table if not exists immigration_staging
    (
        cicid float,
        i94yr float,
        i94mon float,
        i94cit float,
        i94res float,
        i94port varchar,
        arrdate int,
        i94mode float,
        i94addr varchar,
        depdate int,
        i94bir float,
        i94visa float,
        count float,
        dtadfile varchar,
        visapost varchar,
        occup varchar,
        entdepa varchar,
        entdepd varchar,
        entdepu varchar,
        matflag varchar,
        biryear float,
        dtaddto int,
        gender varchar,
        insnum varchar,
        airline varchar,
        admnum float,
        fltno varchar,
        visatype varchar
    )
""")

immigration_table_create = ("""
    create table if not exists immigration
    (
        id int identity(0,1),
        i94yr int,
        i94mon int,
        i94cit_id int references countries(country_id),
        i94res_id int references countries(country_id),
        i94port_id varchar references ports(port_id),
        arrdate date,
        i94mode_id int references modes(mode_id),
        i94addr_id varchar references states(state_id),
        depdate date,
        i94bir int,
        i94visa_id int references visa_categories(visa_id),
        visapost varchar,
        entdepa varchar,
        entdepd varchar,
        biryear int,
        gender varchar,
        airline_id varchar,
        fltno varchar,
        visatype_id varchar references visa_types(visa_type_id),
        constraint immigration_pkey primary key (id)
    )
""")

ports_table_create = ("""
    create table if not exists ports
    (
        port_id varchar,
        port_name varchar,
        port_state varchar,
        constraint ports_pkey primary key (port_id)
    )
""")

countries_table_create = ("""
    create table if not exists countries
    (
        country_id int,
        country_name varchar unique,
        constraint countries_pkey primary key (country_id)
    )
""")

modes_table_create = ("""
    create table if not exists modes
    (
        mode_id int,
        mode_name varchar,
        constraint modes_pkey primary key (mode_id)
    )
""")

states_table_create = ("""
    create table if not exists states
    (
        state_id varchar,
        state_name varchar,
        constraint states_pkey primary key (state_id)
    )
""")

visa_categories_table_create = ("""
    create table if not exists visa_categories
    (
        visa_id int,
        visa_category varchar,
        constraint visa_categories_pkey primary key (visa_id)
    )
""")

visa_types_table_create = ("""
    create table if not exists visa_types
    (
        visa_type_id varchar,
        visa_type varchar,
        constraint visa_types_pkey primary key (visa_type_id)
    )
""")

airlines_table_create = ("""
    create table if not exists airlines
    (
        airline_iata_id varchar,
        airline_icao varchar,
        airline_name varchar,
        airline_callsign varchar,
        country varchar references countries(country_name),
        comment varchar,
        constraint airlines_pkey primary key (airline_iata_id, airline_icao, airline_name)
    )
""")

demographics_table_create = ("""
    create table if not exists demographics
    (
        city varchar,
        state varchar,
        median_age float,
        male_population int,
        female_population int,
        total_population int,
        number_of_veterans int,
        foreign_born int,
        average_household_size float,
        state_code varchar references states(state_id),
        race varchar,
        count int,
        constraint demographics_pkey primary key (city, state_code, race)
    )
""")

# staging tables

staging_copy = ("""
    copy immigration_staging
    from '{}'
    credentials 'aws_iam_role={}'
    format as parquet
""").format(config.get('S3','INPUT_DATA'),
            config.get('IAM_ROLE', 'ARN'))

"""
        id int identity(0,1),
        i94yr int,
        i94mon int,
        i94cit_id int references countries(country_id),
        i94res_id int references countries(country_id),
        i94port_id varchar references ports(port_id),
        arrdate date,
        i94mode_id int references modes(mode_id),
        i94addr_id varchar references states(state_id),
        depdate date,
        i94bir int,
        i94visa_id int references visa_categories(visa_id),
        visapost varchar,
        entdepa varchar,
        entdepd varchar,
        biryear int,
        dtaddto date,
        gender varchar,
        airline_id varchar,
        fltno int,
        visatype_id varchar references visa_types(visa_type_id),
        constraint immigration_pkey primary key (id)

                cicid float,
                i94yr float,
                i94mon float,
                i94cit float,
                i94res float,
                i94port varchar,
                arrdate float,
                i94mode float,
                i94addr varchar,
                depdate float,
                i94bir float,
                i94visa float,
                count float,
                dtadfile varchar,
                visapost varchar,
                occup varchar,
                entdepa varchar,
                entdepd varchar,
                entdepu varchar,
                matflag varchar,
                biryear float,
                dtaddto varchar,
                gender varchar,
                insnum varchar,
                airline varchar,
                admnum float,
                fltno varchar,
                visatype varchar
"""

# insert queries
immigration_table_insert = ("""
    insert into immigration
    (
        i94yr, i94mon, i94cit_id, i94res_id, i94port_id, arrdate,
        i94mode_id, i94addr_id, depdate, i94bir, i94visa_id, visapost, entdepa,
        entdepd, biryear, gender, airline_id, fltno, visatype_id
    )
    select
        cast(i94yr as integer), cast(i94mon as integer), cast(i94cit as integer),
        cast(i94res as integer), i94port,
        dateadd(day,round(arrdate)::int,'19600101'),
        cast(i94mode as integer),
        i94addr,
        dateadd(day,round(depdate)::int,'19600101'),
        cast(i94bir as integer), cast(i94visa as integer),
        visapost, entdepa, entdepd, cast(biryear as integer), gender, airline,
        fltno, visatype
    from immigration_staging
""")


# run queries

drop_table_queries = [immigration_staging_table_drop,
                      immigration_table_drop,
                      ports_table_drop,
                      countries_table_drop,
                      modes_table_drop,
                      states_table_drop,
                      visa_categories_table_drop,
                      visa_types_table_drop,
                      airlines_table_drop,
                      demographics_table_drop
                     ]

create_table_queries = [ports_table_create,
                        countries_table_create,
                        modes_table_create,
                        states_table_create,
                        visa_categories_table_create,
                        visa_types_table_create,
                        airlines_table_create,
                        demographics_table_create,
                        immigration_staging_table_create,
                        immigration_table_create
                       ]

staging_copy_queries = [staging_copy]



insert_table_queries = [immigration_table_insert]
#insert_table_queries = [songplay_table_insert,
#                        user_table_insert,
#                        song_table_insert,
#                        artist_table_insert,
#                        time_table_insert]
