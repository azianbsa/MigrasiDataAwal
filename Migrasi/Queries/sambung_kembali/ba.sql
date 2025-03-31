DROP TEMPORARY TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TEMPORARY TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

SELECT
@idpdam AS idpdam,
p.id AS idpermohonan,
ba.`nomorba` AS nomorba,
ba.`tanggalba` AS tanggalba,
NULL AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS `fotobukti4`,
NULL AS `fotobukti5`,
NULL AS `fotobukti6`,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
NULL AS flag_dari_verifikasi,
ba.`memo` AS statusberitaacara,
ba.`tanggalba` AS waktuupdate
FROM __tmp_sambung_kembali p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL