DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;

CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
  @idpdam,
  @id := @id + 1 AS iduser,
  a.nama,
  a.namauser
FROM
  (SELECT
    nama,
    namauser,
    `passworduser`,
    alamat,
    aktif
  FROM
    [bacameter].`userakses`
  UNION
  SELECT
    nama,
    namauser,
    `passworduser`,
    NULL AS alamat,
    aktif
  FROM
    [bsbs].`userakses`
  UNION
  SELECT
    nama,
    namauser,
    `passworduser`,
    NULL AS alamat,
    flagaktif AS aktif
  FROM
    `userloket`
  UNION
  SELECT
    nama,
    namauser,
    `passworduser`,
    NULL AS alamat,
    flagaktif AS aktif
  FROM
    `userbshl`) a,
  (SELECT
    @id := 0) AS id
GROUP BY a.namauser;

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

SELECT
  @idpdam AS `idpdam`,
  kk.id AS `idkoreksi`,
  0 AS idpermohonan,
  'Manual' AS `sumberperubahan`,
  DATE (`tanggal_koreksi`) AS `waktukoreksi`,
  TIME (`tanggal_koreksi`) AS `jamkoreksi`,
  COALESCE (u.iduser, - 1) AS `iduser`,
  p.`id` AS `idpelangganair`,
  IF (
    `tanggal_verifikasi` IS NOT NULL
    AND `flagprosesdata` = 1,
    1,
    0
  ) AS `flagverifikasi`,
  `tanggal_verifikasi` AS `waktuverifikasi`,
  COALESCE (
    `tanggal_verifikasi`,
    `tanggal_koreksi`,
    NOW()
  ) AS `waktuupdate`
FROM
  `permohonan_koreksi_data` k
  JOIN __tmp_koreksi_data kk
    ON kk.nomor = k.`nomor`
  JOIN pelanggan p
    ON p.`nosamb` = k.`nosamb`
  LEFT JOIN __tmp_userloket u
    ON u.nama = k.`user_koreksi`