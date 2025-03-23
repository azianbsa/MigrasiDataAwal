DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_tutup_total;
CREATE TEMPORARY TABLE __tmp_tutup_total AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_pemutusan_sementara`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

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
v.`waktuverifikasi` AS waktuverifikasi,
0 AS flagusulan,
IF(v.`waktuverifikasi` IS NULL,
 IF(ba.`tanggalba` IS NULL,
  IF(spk.`tanggalspk` IS NULL,
   IF(COALESCE(n.`waktubayar`,nn.`waktubayar`) IS NULL,
    'Menunggu Pelunasan Reguler',
    'Menunggu SPK Pemasangan'),
   'Menunggu Berita Acara'),
  'Menunggu Verifikasi'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
COALESCE(v.`waktuverifikasi`,pp.`tanggal`) AS waktuupdate
FROM __tmp_tutup_total p
JOIN `permohonan_pemutusan_sementara` pp ON pp.nomor=p.nomor
JOIN pelanggan pel ON pel.nosamb=pp.nosamb
LEFT JOIN [bsbs].rayon r ON r.koderayon=pp.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=pp.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=pp.kodegol AND g.aktif=1
LEFT JOIN nonair n ON n.urutan=pp.urutannonair
LEFT JOIN `nonair2022` nn ON nn.`urutan`=pp.`urutannonair`
LEFT JOIN `spk_pemutusan_sementara` spk ON spk.nomorpermohonan=pp.nomor AND spk.flaghapus=0
LEFT JOIN `ba_pemutusan_sementara` ba ON ba.nomorpermohonan=pp.nomor AND ba.flaghapus=0
LEFT JOIN `verifikasi` v ON v.`nomorba`=ba.`nomorba`
,(SELECT @id:=@lastid) AS id