DROP TEMPORARY TABLE IF EXISTS __tmp_kolektif;

CREATE TEMPORARY TABLE __tmp_kolektif AS
SELECT
  @id := @id + 1 AS id,
  kodekolektif
FROM
  kolektif,
  (SELECT
    @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_kondisimeter;

CREATE TEMPORARY TABLE __tmp_kondisimeter AS
SELECT
  @id := @id + 1 AS id,
  kodekondisi,
  kondisi
FROM
  kondisimeter,
  (SELECT
    @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_koreksi_data;
CREATE TEMPORARY TABLE __tmp_koreksi_data AS
SELECT
  nomor,
  @id := @id + 1 AS id
FROM
  `permohonan_koreksi_data`,
  (SELECT
    @id := @lastid) AS id
WHERE flaghapus = 0;

DROP TEMPORARY TABLE IF EXISTS __tmp_koreksi_data_detail;
CREATE TEMPORARY TABLE __TMP_KOREKSI_DATA_DETAIL AS
SELECT
  @iddetail := @iddetail + 1 as `id`,
  @idpdam as `idpdam`,
  k.id as `idkoreksi`,
  a.`parameter`,
  a.`lama`,
  a.`baru`,
  a.`valueid`
FROM
  (SELECT
    `nomor`,
    'Nama' AS parameter,
    `nama_lama` AS lama,
    `nama_baru` AS baru,
    NULL AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `nama_lama` IS NOT NULL
        OR `nama_baru` IS NOT NULL
      )
      and (nama_lama <> nama_baru)
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'Alamat' AS parameter,
    `alamat_lama` AS lama,
    `alamat_baru` AS baru,
    NULL AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `alamat_lama` IS NOT NULL
        OR alamat_baru IS NOT NULL
      )
      and (alamat_lama <> alamat_baru)
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'Flag' AS parameter,
    case
      when lama = '1'
      then 'Normal'
      WHEN lama = '2'
      THEN 'Terpusat/Tanpa Denda'
      WHEN lama = '3'
      THEN 'Air Tidak Mengalir'
      WHEN lama = '4'
      THEN 'Hapus Secara Akuntansi'
      WHEN lama = '5'
      THEN 'Tidak Direkeningkan'
    end as lama,
    CASE
      WHEN baru = '1'
      THEN 'Normal'
      WHEN baru = '2'
      THEN 'Terpusat/Tanpa Denda'
      WHEN baru = '3'
      THEN 'Air Tidak Mengalir'
      WHEN baru = '4'
      THEN 'Hapus Secara Akuntansi'
      WHEN baru = '5'
      THEN 'Tidak Direkeningkan'
    END AS baru,
    baru AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (`lama` IS NOT NULL
        OR baru IS NOT NULL)
      and (lama <> baru)
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'No.Hp' AS parameter,
    `hp_lama` AS lama,
    `hp_baru` AS baru,
    NULL AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `hp_lama` IS NOT NULL
        OR hp_baru IS NOT NULL
      )
      and (hp_lama <> hp_baru)
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'Kolektif' AS parameter,
    `kodekolektif_lama` AS lama,
    `kodekolektif_baru` AS baru,
    - 1 AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `kodekolektif_lama` IS NOT NULL
        OR kodekolektif_baru IS NOT NULL
      )
      and (
        kodekolektif_lama <> kodekolektif_baru
      )
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'Kondisi Meter' AS parameter,
    `kodekondisimeter_lama`,
    `kodekondisimeter_baru`,
    - 1 AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `kodekondisimeter_lama` IS NOT NULL
        OR kodekondisimeter_baru IS NOT NULL
      )
      and (
        kodekondisimeter_lama <> kodekondisimeter_baru
      )
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'No.SeriMeter' AS parameter,
    `serimeter_lama`,
    `serimeter_baru`,
    null AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `serimeter_lama` IS NOT NULL
        OR serimeter_baru IS NOT NULL
      )
      and (serimeter_lama <> serimeter_baru)
    )
  UNION
  ALL
  SELECT
    `nomor`,
    'Daya Listrik' AS parameter,
    `dayalistrik_lama`,
    `dayalistrik_baru`,
    null AS valueid
  FROM
    `permohonan_koreksi_data`
  WHERE `flaghapus` = 0
    AND (
      (
        `dayalistrik_lama` IS NOT NULL
        OR dayalistrik_baru IS NOT NULL
      )
      AND (
        dayalistrik_lama <> dayalistrik_baru
      )
    )) a
  join __tmp_koreksi_data k
    on k.nomor = a.nomor,
  (select
    @iddetail := @lastiddetail) as iddetail
WHERE a.baru <> 'notchanges';

update
  __tmp_koreksi_data_detail a
  left join __tmp_kolektif b
    on b.kodekolektif = a.baru set a.valueid = b.id
where a.parameter = 'Kolektif';

UPDATE
  __tmp_koreksi_data_detail a
  LEFT JOIN __tmp_kondisimeter b
    ON b.kodekondisi = a.baru SET a.valueid = b.id
WHERE a.parameter = 'Kondisi Meter';

select
  *
from
  __tmp_koreksi_data_detail

