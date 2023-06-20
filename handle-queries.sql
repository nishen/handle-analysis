select *
  from handles
 order by url;

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


select 'DELETE ' || h.handle || char(10)
  from handles h
 where datastream is not null;


select *
  from handles
 where datastream is not null;


select *
  from handles h
  join pure p on p.url = h.url;


select *
  from handles h
 where h.alias is not null;


delete from handles;



select *
  from handles_pre_changes hpc
 where hpc.handle in ('1959.14/1252720',
'1959.14/1271014',
'1959.14/1262251',
'1959.14/1261532',
'1959.14/1067047',
'1959.14/62731',
'1959.14/1073969',
'1959.14/228408',
'1959.14/215247',
'1959.14/221878',
'1959.14/385',
'1959.14/714',
'1959.14/264212',
'1959.14/1268396',
'1959.14/1268461',
'1959.14/1070087',
'1959.14/1260351',
'1959.14/176445',
'1959.14/1068038',
'1959.14/1274333',
'1959.14/1268224',
'1959.14/324110',
'1959.14/290675',
'1959.14/1265697',
'1959.14/1068538',
'1959.14/1266499',
'1959.14/1271340',
'1959.14/192192',
'1959.14/1266273',
'1959.14/177768',
'1959.14/1273018',
'1959.14/1273366',
'1959.14/1278985',
'1959.14/1279186',
'1959.14/1150597',
'1959.14/1278553',
'1959.14/1278814',
'1959.14/1141419',
'1959.14/1269871',
'1959.14/229985',
'1959.14/1280570',
'1959.14/1281231',
'1959.14/1269249',
'1959.14/1283613',
'1959.14/1283927',
'1959.14/1282461',
'1959.14/1282803',
'1959.14/1282760',
'1959.14/1054442',
'1959.14/1282625',
'1959.14/1276712',
'1959.14/1284306',
'1959.14/1281913',
'1959.14/1089441',
'1959.14/1089314');