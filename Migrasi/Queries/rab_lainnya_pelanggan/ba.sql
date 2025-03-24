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
a.`nomorba`,
a.`tglpasang`,
- 1 AS iduser,
namapaketrab_persil AS namapaketpersil,
IFNULL(dialihkankevendor, 0) AS persilflagdialihkankevendor,
IFNULL(biayadibebankankepdam, 0) AS persilflagbiayadibebankankepdam,
0 AS persilsubtotal,
0 AS persildibebankankepdam,
IF(
namapaketrab_persil != "",
grandtotal,
0
) AS persiltotal,
namapaketrab AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagdibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
0 AS flagdariverifikasi,
"Berhasil Dikerjakan",
NOW() 
FROM temp_permohonan b
JOIN rab_lainnya a ON a.norab = b.nomorpermohonan 
WHERE jenis IN ("JNS-101", "JNS-29", "JNS-23") 
AND nomorba IS NOT NULL