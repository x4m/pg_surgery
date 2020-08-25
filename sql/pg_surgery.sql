create extension pg_surgery;

--
-- check that using heap_force_kill and heap_force_freeze functions with the
-- supported relations passes.
--

-- normal heap table.
begin;
create table htab(a int);
insert into htab values (100), (200), (300), (400), (500);

select * from htab where xmin = 2;
select heap_force_freeze('htab'::regclass, ARRAY['(0, 4)']::tid[]);
select ctid, xmax from htab where xmin = 2;

select heap_force_kill('htab'::regclass, ARRAY['(0, 4)']::tid[]);
select * from htab where ctid = '(0, 4)';
rollback;

-- materialized view.
begin;
create materialized view mvw as select a from generate_series(1, 3) a;

select * from mvw where xmin = 2;
select heap_force_freeze('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where xmin = 2;

select heap_force_kill('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where ctid = '(0, 3)';
rollback;

--
-- check that using heap_force_kill and heap_force_freeze functions with the
-- unsupported relations fails.
--

-- partitioned tables (the parent table) doesn't contain any tuple.
create table ptab (a int) partition by list (a);

select heap_force_kill('ptab'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('ptab'::regclass, ARRAY['(0, 1)']::tid[]);

create index ptab_idx on ptab (a);

-- indexes are not supported, should fail.
select heap_force_kill('ptab_idx'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('ptab_idx'::regclass, ARRAY['(0, 1)']::tid[]);

create view vw as select 1;

-- views are not supported as well. so, all these should fail.
select heap_force_kill('vw'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('vw'::regclass, ARRAY['(0, 1)']::tid[]);

create sequence seq;

-- sequences are not supported as well. so, all these functions should fail.
select heap_force_kill('seq'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('seq'::regclass, ARRAY['(0, 1)']::tid[]);

--
-- Some negative test-cases with invalid inputs.
--
begin;
create table htab(a int);

-- invalid block number, should be skipped.
select heap_force_kill('htab'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('htab'::regclass, ARRAY['(0, 1)']::tid[]);

insert into htab values (10);

-- invalid offset number, should be skipped.
select heap_force_kill('htab'::regclass, ARRAY['(0, 2)']::tid[]);
select heap_force_freeze('htab'::regclass, ARRAY['(0, 2)']::tid[]);

-- dead tuple, should be skipped.
select heap_force_kill('htab'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_kill('htab'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('htab'::regclass, ARRAY['(0, 1)']::tid[]);

rollback;

-- cleanup.
drop table ptab;
drop view vw;
drop sequence seq;
drop extension pg_surgery;
