DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TABLE IF EXISTS __tmp_koreksi_rek;
CREATE TABLE __tmp_koreksi_rek AS
SELECT 
@id := @id+1 AS id,
a.nomor,
CAST(CONCAT(a.status,'[',COUNT(*),']') AS CHAR) AS `status`
FROM (
SELECT
p.`nomor`,
IF(d.`id` IS NULL,
 'Menunggu Usulan Koreksi',
 '(Selesai) Sudah Verifikasi Pusat') AS `status`
FROM `permohonan_koreksi_rek` p
LEFT JOIN `ba_usulan_koreksi_rekening_periode` d ON d.`nomorpermohonan`=p.`nomor`
WHERE p.`flaghapus`=0 ) a
,(SELECT @id := @lastid) AS id
GROUP BY a.nomor,a.status;

SELECT
@idpdam AS idpdam,
d.`id` AS idpermohonan,
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
FROM __tmp_koreksi_rek d
JOIN`permohonan_koreksi_rek` p ON p.nomor=d.nomor
JOIN pelanggan pel ON pel.nosamb=p.nosamb
LEFT JOIN __tmp_golongan gol ON gol.kodegol=p.kodegol AND gol.aktif=1
LEFT JOIN [bsbs].rayon ray ON ray.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan=p.kodekelurahan