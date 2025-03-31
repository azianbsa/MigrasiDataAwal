DROP TABLE IF EXISTS __tmp_buka_segel;
CREATE TABLE __tmp_buka_segel AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_bukasegel`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS idpdam,
p.`id` AS idpermohonan,
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
FROM __tmp_buka_segel p
JOIN `ba_bukasegel` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0