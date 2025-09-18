DROP TEMPORARY TABLE IF EXISTS temp_permohonan;
CREATE TEMPORARY TABLE temp_permohonan AS 
SELECT 
@id := @id + 1 AS idpermohonan, 
norab AS nomorpermohonan
FROM rab_lainnya
,(SELECT @id:=@lastid) AS id
WHERE rabpengaduan = 0 AND pelanggan="AIR"
AND jenis != "JNS-34" 
AND flaghapus = 0;

SELECT 
a.idpdam,
idpermohonan,
parameter,
tipedata,
CASE
WHEN parameter="Serimeter Baru"  THEN serimeter
WHEN parameter="Keterangan"  THEN keterangan
WHEN parameter="Meteran Diganti" THEN ""
WHEN parameter="Keterangan Lapangan" THEN ""
WHEN parameter="No Segel Baru" THEN ""
ELSE ""
END
AS valuestring,
CASE
WHEN parameter="Stan Angkat"  THEN 0
WHEN parameter="Angka Meter Baru"  THEN stanawalpasang
ELSE 0
END
AS valuedecimal,
CASE
WHEN parameter="Stan Angkat"  THEN 0
WHEN parameter="Angka Meter Baru"  THEN stanawalpasang
ELSE 0
END
AS valuedecimal,
CASE
WHEN parameter="Merk Meter Baru"  THEN 0
WHEN parameter="Diameter Baru"  THEN 0
ELSE 0
END
AS  valueinteger,
CASE
WHEN parameter="Tanggal Pengerjaan Awal"  THEN tglpasang
WHEN parameter="Tanggal Pengerjaan Akhir"  THEN tglpasang
WHEN parameter="Tanggal Pasang"  THEN tglpasang
ELSE 0
END
AS  valuedate,
NULL AS valuebool
FROM
(SELECT 
@idpdam AS idpdam,
idpermohonan,
serimeter,
keterangan,
tglpasang,
stanawalpasang,
t.idtipepermohonan AS idtipepermohonan 
FROM temp_permohonan b
JOIN rab_lainnya a ON a.norab = b.nomorpermohonan
LEFT JOIN __tmp_tipepermohonan t ON t.kodejenisnonair=a.jenis
WHERE jenis IN ("JNS-101", "JNS-29", "JNS-23") 
AND nomorba IS NOT NULL) a 
LEFT JOIN __tmp_badetail ba 
ON a.idtipepermohonan = ba.idtipepermohonan ;