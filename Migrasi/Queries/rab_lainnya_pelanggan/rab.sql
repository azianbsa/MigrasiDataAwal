﻿DROP TEMPORARY TABLE IF EXISTS temp_permohonan;
CREATE TEMPORARY TABLE temp_permohonan AS 
SELECT 
@idpermohonan := @idpermohonan + 1 AS idpermohonan, 
norab AS nomorpermohonan
FROM rab_lainnya 
WHERE rabpengaduan = 0 AND pelanggan="AIR"
AND jenis != "JNS-34" 
AND flaghapus = 0;

SELECT 
@idpdam,
idpermohonan,
t.idjenisnonair AS idjenisnonair,
NULL AS idnonair,
norab,
tglrab,
nomorbppi,
tanggalbppi,
- 1 AS iduser,
validdate,
namapaketrab_persil AS namapaketpersil,
IFNULL(dialihkankevendor,0) AS persilflagdialihkankevendor,
IFNULL(biayadibebankankepdam,0) AS persilflagbiayadibebankankepdam,
0 AS persilsubtotal,
0 AS persildibebankankepdam,
IF(namapaketrab_persil!="",grandtotal,0) AS persiltotal,
namapaketrab AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagdibebankankepdam,
IF(namapaketrab!="",grandtotal,0) AS distribusitotal,
NULL AS fotodenah1,
NULL AS fotodenah2,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
0 AS rekapsubtotal,
0 AS rekapdibebankankepdam,
grandtotal AS rekaptotal,
1 AS flagrablainnya,
0 AS flagbatal,
NULL AS idalasanbatal,
NOW() 
FROM temp_permohonan b
JOIN rab_lainnya a ON a.norab = b.nomorpermohonan
LEFT JOIN __tmp_tipepermohonan t ON t.kodejenisnonair=a.jenis
WHERE jenis IN ("JNS-101", "JNS-29", "JNS-23")