DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TEMPORARY TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

SELECT
@idpdam AS idpdam,
p.id AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
pp.nomor AS nomorpermohonan,
pp.tanggal AS waktupermohonan,
r.id AS idrayon,
k.id AS idkelurahan,
g.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
pp.keterangan AS keterangan,
NULL AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(v.`waktuverifikasi` IS NULL,0,1) AS flagverifikasi,
v.waktuverifikasi AS waktuverifikasi,
0 AS flagusulan,
IF(v.`waktuverifikasi` IS NULL,
 IF(ba.`tanggalba` IS NULL,
  IF(rab.`tglpasang` IS NULL,
   IF(rab.`tglrab` IS NULL,
    IF(spk.`tglspko` IS NULL,
     'Menunggu SPK Survey',
     'Menunggu RAB'),
    'Menunggu SPK Pemasangan'),
   'Menunggu Berita Acara'),
  'Menunggu Verifikasi'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
COALESCE(ba.tanggalba,rab.`tglpasang`,rab.`tglrab`,spk.tglspko,NOW()) waktuupdate
FROM __tmp_sambung_kembali p
JOIN permohonan_sambung_kembali pp ON pp.nomor=p.nomor
JOIN pelanggan pel ON pel.nosamb=pp.nosamb
LEFT JOIN [bsbs].rayon r ON r.koderayon=pp.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=pp.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=pp.kodegol AND g.aktif=1
LEFT JOIN `spk_opname_sambung_kembali` spk ON spk.`nomorpermohonan`=pp.`nomor`
LEFT JOIN `rab_sambung_kembali` rab ON rab.`nomorpermohonan`=pp.`nomor`
LEFT JOIN `ba_sambungkembali` ba ON ba.`nomorpermohonan`=pp.`nomor`
LEFT JOIN verifikasi v ON v.nomorba=ba.nomorba
WHERE spk.`flaghapus`=0 AND rab.`flaghapus`=0 AND ba.`flaghapus`=0;