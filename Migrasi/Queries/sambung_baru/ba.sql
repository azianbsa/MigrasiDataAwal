﻿DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS idpdam,
p.`id` AS idpermohonan,
ba.`nomorba` AS nomorba,
COALESCE(ba.`tanggalba`,ba.`tglpasang`) AS tanggalba,
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
0 AS flagbatal,
NULL AS idalasanbatal,
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
NOW() AS waktuupdate
FROM __tmp_pendaftaran p
JOIN `rab` ba ON ba.`nomorreg`=p.`nomorreg`
WHERE ba.flaghapus=0 AND ba.`nomorba` IS NOT NULL AND ba.`tanggalba` IS NOT NULL