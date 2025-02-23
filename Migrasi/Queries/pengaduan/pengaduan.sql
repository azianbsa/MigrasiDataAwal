DROP TEMPORARY TABLE IF EXISTS __tmp_sumberpengaduan;
CREATE TEMPORARY TABLE __tmp_sumberpengaduan AS
SELECT
@id := @id+1 AS id,
sumberpengaduan
FROM sumberpengaduan
,(SELECT @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser;

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

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
IF(ba.nomorba IS NOT NULL,
 'Selesai',
 IF(ba.nomorspk IS NOT NULL,
  'Menunggu Berita Acara',
  'Menunggu SPK Pemasangan')) AS statuspermohonan,
0 AS flaghapus,
pgd.tglditerima waktuupdate
FROM pengaduan pgd
JOIN pelanggan pel ON pel.nosamb = pgd.nosamb
JOIN spk_pengaduan ba ON ba.nomorpengaduan = pgd.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama = pgd.user
LEFT JOIN __tmp_sumberpengaduan spg ON spg.sumberpengaduan = pgd.sumberpengaduan
LEFT JOIN __tmp_tipepermohonan tpr ON tpr.kodejenisnonair = pgd.kategori
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = pgd.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pgd.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol = pgd.kodegol AND gol.aktif = 1
WHERE pgd.flaghapus = 0