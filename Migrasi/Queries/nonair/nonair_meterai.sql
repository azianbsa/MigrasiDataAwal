SET @idjns = (SELECT `idjenisnonair` FROM `master_attribute_jenis_nonair` WHERE idpdam=@idpdam AND `kodejenisnonair`='JNS-16' AND `flaghapus`=0);

REPLACE INTO `rekening_nonair_detail`
SELECT
a.`idpdam`,
SUBSTRING_INDEX(GROUP_CONCAT(a.`idnonair` ORDER BY a.`idnonair`),',',1) AS idnonair,
'Meterai' AS parameter,
'meterai' AS postbiaya,
b.total AS `value`,
NOW() AS waktuupdate
FROM rekening_nonair a
JOIN (
SELECT `nomornonair`,`total` FROM `rekening_nonair` WHERE idpdam=@idpdam AND `idjenisnonair`=@idjns
) b ON b.nomornonair=a.`nomornonair`
WHERE a.idpdam=@idpdam
GROUP BY a.`nomornonair`