DROP TEMPORARY TABLE IF EXISTS temp_permohonan;
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
nomorspkp,
tanggalspkp,
nomorsppb,
NULL AS tanggalsppb,
-1 AS iduser,
NULL AS fotobukti1, 
NULL AS fotobukti2,
NULL AS fotobukti3,
0 AS flagbatal,
NULL AS idalasanbatal,
NOW()
FROM temp_permohonan b 
JOIN rab_lainnya a ON a.norab=b.nomorpermohonan
WHERE 
jenis NOT IN  ("JNS-34","JNS-145","JNS-138","JNS-141") AND a.`nomorspkp` IS NOT NULL