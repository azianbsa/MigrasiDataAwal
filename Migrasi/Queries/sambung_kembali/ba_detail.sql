SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
r.`keteranganmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Kondisi Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
k.`idkondisimeter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kondisi_meter` k ON k.`kodekondisimeter`=r.`kondisimeter`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Merek Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.`idmerekmeter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_merek_meter` m ON m.namamerekmeter=r.`merkmeter`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'No Segel' AS `parameter`,
'string' AS `tipedata`,
r.`nosegelmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Seri Meter' AS `parameter`,
'string' AS `tipedata`,
r.`serimeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Stan Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
r.`stanawalpasang` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM `rab_sambung_kembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1