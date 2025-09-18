-- master_pelanggan_air_riwayat_koreksi_detail
-- new(0, "id")
-- new(1, "idpdam")
-- new(2, "idkoreksi")
-- new(3, "parameter")
-- new(4, "lama")
-- new(5, "baru")
-- new(6, "valueid")

DROP TEMPORARY TABLE IF EXISTS __tmp_koreksi_data_detail;
CREATE TEMPORARY TABLE __tmp_koreksi_data_detail AS
SELECT
@id:=@id+1 AS `id`,
@idpdam AS `idpdam`,
pp.idkoreksi AS `idkoreksi`,
p.`parameter` AS parameter,
p.`lama` AS lama,
p.`baru` AS baru,
p.`valueid` AS valueid
FROM (
SELECT
a.`nomor`,
'Nama' AS parameter,
`nama_lama` AS lama,
`nama_baru` AS baru,
NULL AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`nama_lama` IS NOT NULL OR `nama_baru` IS NOT NULL) AND (nama_lama<>nama_baru))
UNION ALL
SELECT
a.`nomor`,
'Alamat' AS parameter,
`alamat_lama` AS lama,
`alamat_baru` AS baru,
NULL AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`alamat_lama` IS NOT NULL OR alamat_baru IS NOT NULL) AND (alamat_lama<>alamat_baru))
UNION ALL
SELECT
a.`nomor`,
'Flag' AS parameter,
CASE WHEN lama='1' THEN 'Normal'
WHEN lama='2' THEN 'Terpusat/Tanpa Denda'
WHEN lama='3' THEN 'Air Tidak Mengalir'
WHEN lama='4' THEN 'Hapus Secara Akuntansi'
WHEN lama='5' THEN 'Tidak Direkeningkan'
END AS lama,
CASE
WHEN baru='1' THEN 'Normal' WHEN baru='2' THEN 'Terpusat/Tanpa Denda'
WHEN baru='3' THEN 'Air Tidak Mengalir'
WHEN baru='4' THEN 'Hapus Secara Akuntansi'
WHEN baru='5' THEN 'Tidak Direkeningkan'
END AS baru,
baru AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`lama` IS NOT NULL OR baru IS NOT NULL) AND (lama<>baru))
UNION ALL
SELECT
a.`nomor`,
'No.Hp' AS parameter,
`hp_lama` AS lama,
`hp_baru` AS baru,
NULL AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`hp_lama` IS NOT NULL OR hp_baru IS NOT NULL) AND (hp_lama<>hp_baru))
UNION ALL
SELECT
a.`nomor`,
'Kolektif' AS parameter,
`kodekolektif_lama` AS lama,
`kodekolektif_baru` AS baru,
-1 AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`kodekolektif_lama` IS NOT NULL OR kodekolektif_baru IS NOT NULL) AND (kodekolektif_lama<>kodekolektif_baru))
UNION ALL
SELECT
a.`nomor`,
'Kondisi Meter' AS parameter,
`kodekondisimeter_lama`,
`kodekondisimeter_baru`,
-1 AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`kodekondisimeter_lama` IS NOT NULL OR kodekondisimeter_baru IS NOT NULL) AND (kodekondisimeter_lama<>kodekondisimeter_baru))
UNION ALL
SELECT
a.`nomor`,
'No.SeriMeter' AS parameter,
`serimeter_lama`,
`serimeter_baru`,
NULL AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`serimeter_lama` IS NOT NULL OR serimeter_baru IS NOT NULL) AND (serimeter_lama<>serimeter_baru))
UNION ALL
SELECT
a.`nomor`,
'Daya Listrik' AS parameter,
`dayalistrik_lama`,
`dayalistrik_baru`,
NULL AS valueid
FROM `permohonan_koreksi_data` a
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` b ON b.nomor=a.nomor
AND ((`dayalistrik_lama` IS NOT NULL OR dayalistrik_baru IS NOT NULL) AND (dayalistrik_lama<>dayalistrik_baru))) p
JOIN `kotaparepare_dataawal`.`tampung_koreksi_data` pp ON pp.nomor=p.nomor,
(SELECT @id:=@lastid) AS id
WHERE p.baru<>'notchanges';

UPDATE __tmp_koreksi_data_detail a
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kolektif` b ON b.kodekolektif=a.baru 
SET a.valueid=b.`idkolektif`
WHERE a.parameter='Kolektif';

UPDATE __tmp_koreksi_data_detail a
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kondisi_meter` b ON b.`kodekondisimeter`=a.baru 
SET a.valueid=b.`idkondisimeter`
WHERE a.parameter='Kondisi Meter';

SELECT * FROM __tmp_koreksi_data_detail