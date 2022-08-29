select *
  from handles;

select *
  from figshare;

select *
  from pure;

select *
  from handles h
  join figshare f on h.handle = f.handle;

select 'MODIFY ' || h.handle || char(10) ||
       h.url_index || ' URL 86400 1110 UTF8 ' || p.url || char(10)
  from handles h
  join pure p on p.pid = h.pid
 where h.datastream is null
   and host = 'minerva.mq.edu.au'
   and port = 8080
 order by h.pid, h.handle;


select *
  from handles h
  join pure p on p.pid = h.pid
 where h.datastream is null
   and host = 'minerva.mq.edu.au'
   and port = 8080
   and h.pid in (select ih.pid
                   from handles ih
                   join pure ip on ip.pid = ih.pid
                  where ih.datastream is null
                    and ih.host = 'minerva.mq.edu.au'
                    and ih.port = 8080
                  group by ih.pid
                 having count(*) > 1)
 order by h.pid, h.handle;

select *
  from handles h
  join pure p on p.pid = h.pid
 where h.datastream is null
   and host = 'minerva.mq.edu.au'
   and port = 8080
   and h.pid in (select ih.pid
                   from handles ih
                   join pure ip on ip.pid = ih.pid
                  where ih.datastream is null
                    and ih.host = 'minerva.mq.edu.au'
                    and ih.port = 8080
                  group by ih.pid
                 having count(*) = 1)
 order by h.pid, h.handle;



select h.pid
  from handles h
  join pure p on p.pid = h.pid
 where h.datastream is null
   and host = 'minerva.mq.edu.au'
   and port = 8080
 group by h.pid
 having count(*) > 1;


select distinct ih.pid
  from handles ih
  join pure ip on ip.pid = ih.pid
 where ih.datastream is null
   and ih.host = 'minerva.mq.edu.au'
   and ih.port = 8080
 group by ih.pid
having count(*) > 1;


select 'MODIFY ' || h.handle || char(10) ||
       h.url_index || ' URL 86400 1110 UTF8 ' || f.url || char(10) ||
       char(10) ||
       'ADD ' || h.handle || char(10) ||
       '20 HS_ALIAS 86400 1110 UTF8 ' || f.doi || char(10)
  from handles h
  join figshare f on h.handle = f.handle;


select 'MODIFY ' || h.handle || char(10) ||
       h.url_index || ' URL 86400 1110 UTF8 ' || f.url || char(10) ||
       '20 HS_ALIAS 86400 1110 UTF8 https://doi.org/' || f.doi || char(10)
  from handles_pre_changes h
  join figshare f on h.handle = f.handle;


select 'DELETE ' || h.handle || char(10)
  from handles h
 where h.host = 'minerva64';


select 'DELETE ' || h.handle
  from handles h
 where scheme is null
   and url not like '%researchfinder%';


select *
  from handles
 where datastream is null;


select *
  from handles h
  join pure p on p.url = h.url;


select *
  from handles h
 where h.alias is not null;
