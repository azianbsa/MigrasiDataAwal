new(0, "idpdam")
new(1, "idpermohonan")
new(2, "parameter")
new(3, "tipedata")
new(4, "valuestring")
new(5, "valuedecimal")
new(6, "valueinteger")
new(7, "valuedate")
new(8, "valuebool")
new(9, "waktuupdate")

SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Diameter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`iddiameter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
LEFT JOIN [dataawal].`master_tarif_diameter` d ON d.`kodediameter`=pp.`pipa_instalasi` AND d.idpdam=@idpdam
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Diameter Distribusi' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`iddiameter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
LEFT JOIN [dataawal].`master_tarif_diameter` d ON d.`kodediameter`=pp.`pipa_instalasi` AND d.idpdam=@idpdam
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Golongan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`idgolongan` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
LEFT JOIN [dataawal].`master_tarif_golongan` d ON d.`kodegolongan`=pp.`pipa_instalasi` AND d.idpdam=@idpdam
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Jarak Pipa Distribusi (Meter)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jarak_pipa_dis_ke_meter` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Keterangan Survey' AS `parameter`,
'string' AS `tipedata`,
pp.`keteranganopname` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Peruntukan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
r.idperuntukan AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `pendaftaran` pd ON pd.`nomorreg`=p.`nomorpermohonan`
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
LEFT JOIN kotaparepare_dataawal.`master_attribute_peruntukan` r ON r.namaperuntukan=pd.peruntukan
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Tanggal Realisasi Survey' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
pp.tglspko AS `valuedate`,
NULL AS `valuebool`,
pp.tglspko AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorpermohonan`
WHERE pp.`tglspko` IS NOT NULL