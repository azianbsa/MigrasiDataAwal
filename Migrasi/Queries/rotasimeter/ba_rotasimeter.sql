DROP TEMPORARY TABLE IF EXISTS __tmp_userbshl;
CREATE TEMPORARY TABLE __tmp_userbshl AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM `userbshl`
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

SELECT
@idpdam AS idpdam,
r.`idpermohonan` AS idpermohonan,
p.`no_ba` AS nomorba,
p.`tgl_ba` AS tanggalba,
u.iduser AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
p.`tgl_ba` AS waktuupdate
FROM `rotasimeter` p
JOIN __tmp_rotasimeter r ON r.`nosamb`=p.`nosamb` AND r.`periode`=p.`periode`
LEFT JOIN __tmp_userbshl u ON u.nama=p.`user_ba`
WHERE p.`tgl_ba` IS NOT NULL AND p.`flag_ba`=1