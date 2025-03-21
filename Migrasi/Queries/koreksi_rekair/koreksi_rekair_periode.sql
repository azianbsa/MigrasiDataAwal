﻿DROP TABLE IF EXISTS __tmp_periode;
CREATE TEMPORARY TABLE __tmp_periode
SELECT
@idperiode := @idperiode+1 AS idperiode,
periode
FROM [bsbs].periode
,(SELECT @idperiode := 0) AS idperiode
ORDER BY periode;

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
d.`id` AS `id`,
@idpdam AS `idpdam`,
pl.`id` AS `idpelangganair`,
pr.idperiode AS `idperiode`,
p.`id` AS `idpermohonan`,
d.`tanggalba` AS `waktuusulan`,
1 AS `statusverifikasilapangan`,
d.`tanggalba` AS `waktustatusverifikasilapangan`,
NULL AS `keteranganstatusverifikasilapangan`,
1 AS `statusverifikasipusat`,
d.`tanggalba` AS `waktustatusverifikasipusat`,
NULL AS `keteranganstatusverifikasipusat`,
d.`nomorba` AS `nomorba`,
d.`stanlalu_lama` AS `stanlalu`,
d.`stankini_lama` AS `stanskrg`,
0 AS `stanangkat`,
d.`pakai_lama` AS `pakai`,
d.`biayapemakaian_lama` AS `biayapemakaian`,
d.`administrasi_lama` AS `administrasi`,
d.`pemeliharaan_lama` AS `pemeliharaan`,
d.`retribusi_lama` AS `retribusi`,
0 AS `pelayanan`,
0 AS `airlimbah`,
0 AS `dendapakai0`,
0 AS `administrasilain`,
0 AS `pemeliharaanlain`,
0 AS `retribusilain`,
0 AS `ppn`,
d.`meterai_lama` AS `meterai`,
d.`rekair_lama` AS `rekair`,
0 AS `denda`,
d.`rekair_lama` AS `total`,
0 AS `flaghanyaabonemen`,
d.`stanlalu_baru` AS `stanlalu_usulan`,
d.`stankini_baru` AS `stanskrg_usulan`,
0 AS `stanangkat_usulan`,
d.`pakai_baru` AS `pakai_usulan`,
d.`biayapemakaian_baru` AS `biayapemakaian_usulan`,
d.`administrasi_baru` AS `administrasi_usulan`,
d.`pemeliharaan_baru` AS `pemeliharaan_usulan`,
d.`retribusi_baru` AS `retribusi_usulan`,
0 AS `pelayanan_usulan`,
0 AS `airlimbah_usulan`,
0 AS `dendapakai0_usulan`,
0 AS `administrasilain_usulan`,
0 AS `pemeliharaanlain_usulan`,
0 AS `retribusilain_usulan`,
d.`meterai_baru` AS `meterai_usulan`,
0 AS `ppn_usulan`,
d.`rekair_baru` AS `rekair_usulan`,
0 AS `denda_usulan`,
d.`rekair_baru` AS `total_usulan`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
d.`stanlalu_baru` AS `stanlalu_baru`,
d.`stankini_baru` AS `stanskrg_baru`,
0 AS `stanangkat_baru`,
d.`pakai_baru` AS `pakai_baru`,
d.`biayapemakaian_baru` AS `biayapemakaian_baru`,
d.`administrasi_baru` AS `administrasi_baru`,
d.`pemeliharaan_baru` AS `pemeliharaan_baru`,
d.`rekair_baru` AS `retribusi_baru`,
0 AS `pelayanan_baru`,
0 AS `airlimbah_baru`,
0 AS `dendapakai0_baru`,
0 AS `administrasilain_baru`,
0 AS `pemeliharaanlain_baru`,
0 AS `retribusilain_baru`,
d.`meterai_baru` AS `meterai_baru`,
0 AS `ppn_baru`,
d.`rekair_baru` AS `rekair_baru`,
0 AS `denda_baru`,
d.`rekair_baru` AS `total_baru`,
'(Selesai) Sudah Verifikasi Pusat' AS `statuspermohonan`,
0 AS `flaghapus`,
d.`tanggalba` AS `waktuupdate`
FROM __tmp_koreksi_rek p
JOIN `ba_usulan_koreksi_rekening_periode` d ON d.nomorpermohonan=p.nomor
JOIN pelanggan pl ON pl.nosamb=d.nosamb
JOIN __tmp_periode pr ON pr.periode=d.`periode`
LEFT JOIN __tmp_golongan g ON g.kodegol=d.kodegol AND g.aktif=1
LEFT JOIN [bsbs].rayon r ON r.koderayon=d.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=pl.kodekelurahan