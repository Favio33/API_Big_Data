DROP TABLE IF EXISTS employee.departments CASCADE;
CREATE TABLE employee.departments(
    id integer NOT NULL,
    department character varying(150),
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS employee.jobs CASCADE;
CREATE TABLE employee.jobs(
    id integer NOT NULL,
    job character varying(100),
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS employee.hiredemployees CASCADE;
CREATE TABLE employee.hiredemployees(
    id integer NOT NULL,
    name character varying(100),
    datetime timestamp with time zone,
    department_id integer,
    job_id integer,
    PRIMARY KEY(id)
);