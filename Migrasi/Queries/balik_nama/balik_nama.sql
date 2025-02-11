﻿DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket (
    iduser INT,
    nama VARCHAR(30),
    INDEX idx_tmp_userloket_nama (nama)
);
INSERT INTO __tmp_userloket
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM userloket
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

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
usr.iduser AS iduser,
na.id AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(ba.nomorba IS NULL,0,1) AS flagverifikasi,
ba.tanggalba AS waktuverifikasi,
0 AS flagusulan,
IF(ba.nomorba IS NULL,
 IF(na.flaglunas=0,'Menunggu Pelunasan Reguler','Menunggu Verifikasi'),
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
LEFT JOIN __tmp_userloket usr ON usr.nama = SUBSTRING_INDEX(bn.urutannonair,'.BALIK NAMA.',1)
,(SELECT @id := @lastid) AS id
WHERE bn.flaghapus = 0