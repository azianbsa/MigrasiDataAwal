DROP TEMPORARY TABLE IF EXISTS temp_sumberpengaduan;
CREATE TEMPORARY TABLE temp_sumberpengaduan AS
SELECT
@id := @id+1 AS id,
sumberpengaduan
FROM sumberpengaduan
,(SELECT @id := 0) AS id;

SELECT
  @idpdam,
  pgd.id AS idpermohonan,
  tpr.idtipepermohonan AS idtipepermohonan,
  spg.id AS idsumberpengaduan,
  pgd.nomor AS nomorpermohonan,
  pgd.tglditerima AS waktupermohonan,
  ray.id AS idrayon,
  kel.id AS idkelurahan,
  gol.id AS idgolongan,
  NULL AS iddiameter,
  pel.id idpelangganair,
  pgd.uraianlaporan AS keterangan,
  usr.namauser AS iduser,
  NULL AS idnonair,
  NULL AS latitude,
  NULL AS longitude,
  NULL AS alamatmap,
  NULL AS fotobukti1,
  NULL AS fotobukti2,
  NULL AS fotobukti3,
  0 AS flagverifikasi,
  NULL AS waktuverifikasi,
  0 AS flagusulan,
  NULL AS statuspermohonan,
  0 AS flaghapus,
  pgd.tglditerima waktuupdate
FROM pengaduan pgd
JOIN [bsbs].pelanggan pel ON pel.nosamb = pgd.nosamb
LEFT JOIN userloket usr ON usr.nama = pgd.user
LEFT JOIN temp_sumberpengaduan spg ON spg.sumberpengaduan = pgd.sumberpengaduan
LEFT JOIN temp_dataawal_tipepermohonan tpr ON tpr.kodejenisnonair = pgd.kategori
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = pgd.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pgd.kodekelurahan
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = pgd.kodegol AND gol.aktif = 1
WHERE pgd.flaghapus = 0;