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
j.idjenisnonair AS idtipepermohonan,
0 AS idsumberpengaduan,
a.norab AS nomorpermohonan,
a.tglrab AS waktupermohonan,
- 1 AS idrayon,
NULL AS idkelurahan,
NULL AS idgolongan,
NULL AS iddiameter,
p.id AS idpelangganair,
a.keterangan,
-1 AS iduser,
NULL idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(a.nomorba IS NOT NULL,1,0) AS flagverifikasi,
IF(a.tglpasang IS NULL,NULL,a.tglpasang) AS waktuverifikasi,
0 AS flagusulan,
IF(a.nomorspkp IS NULL,"Menunggu SPK Pemasangan",IF(a.flaglunas=0,"Menunggu Pelunasan Reguler",IF(a.nomorba IS NULL,"Menunggu Berita Acara","SELESAI"))) AS statuspermohonan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM temp_permohonan b
JOIN rab_lainnya a ON a.norab=b.nomorpermohonan
JOIN `pelanggan` p ON p.nosamb=a.nosamb
LEFT JOIN __tmp_tipepermohonan t ON t.kodejenisnonair=a.jenis