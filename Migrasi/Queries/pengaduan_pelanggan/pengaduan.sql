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

DROP TEMPORARY TABLE IF EXISTS __tmp_pengaduan;
CREATE TEMPORARY TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=1;

SELECT
@idpdam,
pp.id AS idpermohonan,
t.idtipepermohonan AS idtipepermohonan,
s.id AS idsumberpengaduan,
p.nomor AS nomorpermohonan,
p.tglditerima AS waktupermohonan,
r.id AS idrayon,
k.id AS idkelurahan,
g.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
p.uraianlaporan AS keterangan,
usr.iduser AS iduser,
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
p.tglditerima AS waktuupdate
FROM
__tmp_pengaduan pp
JOIN pengaduan p ON p.nomor=pp.nomor
JOIN pelanggan pel ON pel.nosamb=p.nosamb
LEFT JOIN spk_pengaduan ba ON ba.nomorpengaduan=p.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=p.user
LEFT JOIN __tmp_sumberpengaduan s ON s.sumberpengaduan=p.sumberpengaduan
LEFT JOIN __tmp_tipepermohonan t ON t.kodejenisnonair=p.kategori
LEFT JOIN [bsbs].rayon r ON r.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=p.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=p.kodegol AND g.aktif=1