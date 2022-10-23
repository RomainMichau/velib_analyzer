-- public.tricount definition

-- Drop table

-- DROP TABLE public.tricount;


CREATE TABLE public.velibs
(
    id         int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
    velib_code int8 NOT NULL,
    electric   boolean,
    CONSTRAINT velib_pk PRIMARY KEY (id),
    CONSTRAINT velib_code_un UNIQUE (velib_code)
);

CREATE TABLE public.stations
(
    id           int8    NOT NULL GENERATED ALWAYS AS IDENTITY,
    station_name varchar NOT NULL,
    long         numeric NOT NULL,
    lat          numeric NOT NULL,
    code         varchar    NOT NULL,

    CONSTRAINT station_pk PRIMARY KEY (id),
    CONSTRAINT code_un UNIQUE (code)
);

CREATE TABLE public.velib_docked
(
    id         int8        NOT NULL GENERATED ALWAYS AS IDENTITY,
    velib_code int8        NOT NULL,
    rate_time  timestamptz NOT NULL,
    station_id int8        NOT NULL,
    CONSTRAINT velib_docked_pk PRIMARY KEY (id),
    CONSTRAINT station_id_fk FOREIGN KEY (station_id) REFERENCES public.stations (id),
    CONSTRAINT velib_code_fk FOREIGN KEY (velib_code) REFERENCES public.velibs (velib_code)

);



CREATE TABLE public.rating
(
    id         int8        NOT NULL GENERATED ALWAYS AS IDENTITY,
    velib_code int8        NOT NULL,
    rate       int8        NOT NULL,
    rate_time  timestamptz NOT NULL,
    station_id int8        NOT NULL,
    CONSTRAINT rating_pk PRIMARY KEY (id),
    CONSTRAINT velib_code_fk FOREIGN KEY (velib_code) REFERENCES public.velibs (velib_code),
    CONSTRAINT station_id_fk FOREIGN KEY (station_id) REFERENCES public.stations (id),
    CONSTRAINT velib_code UNIQUE (velib_code, rate_time)
);

CREATE TABLE public.run
(
    id       int8        NOT NULL GENERATED ALWAYS AS IDENTITY,
    run_time timestamptz NOT NULL,
    CONSTRAINT run_pk PRIMARY KEY (id),
    CONSTRAINT run_time_un UNIQUE (run_time)
);