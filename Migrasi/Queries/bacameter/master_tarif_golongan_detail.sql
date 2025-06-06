﻿SELECT
@idpdam,
@id:=@id+1 AS idgolongan,
administrasi,
pemeliharaan,
pelayanan,
retribusi,
retribusi_pakai AS retribusipakai,
airlimbah,
minpakai,
dendapakai0,
ppn,
bb1,
ba1,
bb2,
ba2,
bb3,
ba3,
bb4,
ba4,
bb5,
ba5,
t1,
t2,
t3,
t4,
t5,
mindenda,
if(t1_tetap=1,1,0) AS t1tetap,
if(t2_tetap=1,1,0) AS t2tetap,
if(t3_tetap=1,1,0) AS t3tetap,
if(t4_tetap=1,1,0) AS t4tetap,
if(t5_tetap=1,1,0) AS t5tetap,
dendatunggakan AS dendatunggakan1,
dendatunggakan2,
dendatunggakan3,
dendatunggakan4,
dendatunggakanperbulan,
trf_denda_berdasarkan_persen AS trfdendaberdasarkanpersen,
0 AS biayaadminberlakupada,
0 AS dendatunggakanperhari,
0 AS maxtotaldendaperhari,
NOW() AS waktuupdate
FROM
golongan,
(SELECT @id:=0) AS id;