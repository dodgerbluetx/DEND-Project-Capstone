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
staging_table_list = ['immigration_staging']

insert_table_queries = [immigration_table_insert]
insert_table_list = ['immigration']
