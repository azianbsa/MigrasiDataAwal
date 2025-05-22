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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
LEFT JOIN [dataawal].`master_attribute_kondisi_meter` k ON k.`kodekondisimeter`=r.`kondisimeter`
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
LEFT JOIN [dataawal].`master_attribute_merek_meter` m ON m.namamerekmeter=r.`merkmeter`
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
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
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Tanggal Pasang' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
r.`tglpasang` AS `valuedate`,
NULL AS `valuebool`,
r.tglpasang AS `waktuupdate`
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=r.`nomorreg`
WHERE r.`flaghapus`=0 AND r.`flagpasang`=1