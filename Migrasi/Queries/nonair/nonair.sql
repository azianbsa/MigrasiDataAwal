SET @maxid=(SELECT COALESCE(MAX(`idnonair`),0) AS maxid FROM [dataawal].`tampung_rekening_nonair` WHERE idpdam=@idpdam);

SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idnonair,
j.`idjenisnonair` AS idjenisnonair,
p.`idpelangganair` AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
IF(n.periode='',NULL,n.periode) AS kodeperiode,
n.`nomor` AS nomornonair,
n.keterangan AS keterangan,
n.total AS total,
n.tglmulaitagih AS tanggalmulaitagih,
n.validdate AS tanggalkadaluarsa,
n.nama AS nama,
n.alamat AS alamat,
r.`idrayon` AS idrayon,
NULL AS idkelurahan,
g.`idgolongan` AS idgolongan,
NULL AS idtariflimbah,
NULL AS idtariflltt,
n.`flagangsur` AS flagangsur,
NULL AS idangsuran,
n.`termin` AS termin,
n.`kwitansimanual` AS flagmanual,
NULL AS idpermohonansambunganbaru,
n.flaghapus AS flaghapus,
u.`iduser` AS `iduser`,
n.`waktuupdate` AS waktuupdate,
n.`waktuinput` AS `created_at`,
n.urutan AS urutan
FROM `nonair` n
LEFT JOIN [dataawal].`tampung_master_pelanggan_air` p ON p.`nosamb`=n.`dibebankankepada` AND p.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_jenis_nonair` j ON j.`kodejenisnonair`=n.`jenis` AND j.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_rayon` r ON r.`koderayon`=n.koderayon AND r.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_tarif_golongan` g ON g.`kodegolongan`=n.`kodegol` AND g.`status`=1 AND g.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_user` u ON u.`nama`=n.`userinput` AND u.`idpdam`=@idpdam
,(SELECT @id:=@maxid) AS id
WHERE n.flaghapus=0
AND n.flagangsur=0
AND n.`nomor` NOT IN (SELECT `nomornonair` FROM [dataawal].`tampung_rekening_nonair` WHERE idpdam=@idpdam)
AND n.`jenis` NOT IN (
'JNS-38', -- denda air
'JNS-16' -- meterai
)
AND DATE_FORMAT(n.`tglmulaitagih`,'%Y%m') BETWEEN 202502 AND 202504;