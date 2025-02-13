SELECT
@idpdam AS idpdam,
@id := @id+1 AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
per.nomor AS nomorpermohonan,
per.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
per.keterangan AS keterangan,
NULL AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(ba.tanggalba IS NULL,0,1) AS flagverifikasi,
ba.tanggalba AS waktuverifikasi,
0 AS flagusulan,
IF(ba.tanggalba IS NULL,
 IF(rab.`tglpasang` IS NULL,
  IF(rab.`tglrab` IS NULL,
   IF(spk.tglspko IS NULL,
    'Menunggu SPK Survey',
    'Menunggu RAB'),
   'Menunggu SPK Pemasangan'),
  'Menunggu Berita Acara'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
COALESCE(ba.tanggalba,rab.`tglpasang`,rab.`tglrab`,spk.tglspko,NOW()) waktuupdate
FROM permohonan_sambung_kembali per
JOIN pelanggan pel ON pel.nosamb = per.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = per.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = per.kodekelurahan
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = per.kodegol AND gol.aktif = 1
LEFT JOIN `spk_opname_sambung_kembali` spk ON spk.`nomorpermohonan`=per.`nomor`
LEFT JOIN `rab_sambung_kembali` rab ON rab.`nomorpermohonan`=per.`nomor`
LEFT JOIN `ba_sambungkembali` ba ON ba.`nomorpermohonan`=per.`nomor`
,(SELECT @id := @lastid) AS id
WHERE per.flaghapus = 0 AND spk.`flaghapus`=0 AND rab.`flaghapus`=0 AND ba.`flaghapus`=0;