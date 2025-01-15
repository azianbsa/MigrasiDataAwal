SELECT
@idpdam as idpdam,
@id := @id+1 AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
bn.nomor AS nomorpermohonan,
bn.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
bn.keterangan AS keterangan,
-1 AS iduser,
na.id AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
0 AS flagverifikasi,
NULL AS waktuverifikasi,
0 AS flagusulan,
IF(ba.nomorba IS NULL,
 IF(na.flaglunas=0,'Menunggu Pelunasan Reguler','Menunggu Berita Acara'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
bn.tanggal waktuupdate
FROM permohonan_balik_nama bn
JOIN [bsbs].pelanggan pel ON pel.nosamb = bn.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = bn.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = bn.kodekelurahan
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = bn.kodegol AND gol.aktif = 1
LEFT JOIN nonair na ON na.urutan = bn.urutannonair
LEFT JOIN ba_balik_nama ba ON ba.nomorpermohonan = bn.nomor AND ba.flaghapus=0
,(SELECT @id := @lastid) AS id
WHERE bn.flaghapus = 0