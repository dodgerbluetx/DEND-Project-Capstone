import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES
immigration_table_drop = "drop table if exists immigration"
ports_table_drop = "drop table if exists ports"
countries_table_drop = "drop table if exists countries"
modes_table_drop = "drop table if exists modes"
states_table_drop = "drop table if exists states"
visa_categories_table_drop = "drop table if exists visa_categories"
visa_types_table_drop = "drop table if exists visa_types"
airlines_table_drop = "drop table if exists airlines"

immigration_table_create = ("""
    create table if not exists immigration
    (
        id int identity(0,1),
        i94yr year,
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
        biryear year,
        dtaddto date,
        gender varchar,
        airline_id varchar references airlines(airline_iata_id),
        fltno int,
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
        country_name varchar,
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
        airline_name varchar,
        airline_alias varchar,
        airline_icao varchar,
        airline_callsign varchar,
        country varchar,
        active varchar,
        constraint airlines_pkey primary key (airline_iata_id)
    )
""")

# run queries

drop_table_queries = [immigration_table_drop,
                      ports_table_drop,
                      countries_table_drop,
                      modes_table_drop,
                      states_table_drop,
                      visa_categories_table_drop,
                      visa_types_table_drop,
                      airlines_table_drop
                     ]

create_table_queries = [ports_table_create,
                        countries_table_create,
                        modes_table_create,
                        states_table_create,
                        visa_categories_table_create,
                        visa_types_table_create,
                        airlines_table_create,
                        immigration_table_create
                       ]
