SELECT
@idpdam AS idpdam,
d.`idpermohonan` AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
p.nomor AS nomorpermohonan,
p.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
p.keterangan AS keterangan,
NULL AS iduser,
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
d.status AS statuspermohonan,
0 AS flaghapus,
p.tanggal AS waktuupdate
FROM `permohonan_koreksi_rek` p
JOIN pelanggan pel ON pel.nosamb = p.nosamb
LEFT JOIN __tmp_koreksi_rek d ON d.`nomor`=p.`nomor`
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = p.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = p.kodekelurahan
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = p.kodegol AND gol.aktif = 1
WHERE p.flaghapus = 0