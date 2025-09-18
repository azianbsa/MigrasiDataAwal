SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
8 AS idtipepermohonan,
NULL AS idsumberpengaduan,
a.`no_reg` AS nomorpermohonan,
a.`tgl_reg` AS waktupermohonan,
1 AS flagpendaftaran,
1 AS idtipependaftaransambungan,
a.`nama` AS nama,
a.`alamat` AS alamat,
COALESCE(g.`idgolongan`,30) idgolongan,
-1 AS iddiameter,
COALESCE(r.`idrayon`,-1) AS idrayon,
1 AS idkelurahan,
1 AS idblok,
COALESCE(bg.`idperuntukan`,1) AS idperuntukan,
-1 AS idjenisbangunan,
1 AS idkepemilikan,
1 AS idpekerjaan,
COALESCE(k.`idkolektif`,1) AS idkolektif,
7 AS idsumberair,
-1 AS iddma,
-1 AS iddmz,
-1 AS idmerekmeter,
1 AS idkondisimeter,
1 AS idadministrasilain,
1 AS idpemeliharaanlain,
1 AS idretribusilain,
a.`no_met` AS noserimeter,
a.`tgl_met` AS tglmeter,
0 AS urutanbaca,
0 AS stanawalpasang,
a.`telp` AS notelp,
NULL AS email,
a.`ktp` AS noktp,
NULL AS nokk,
a.`kodepos` AS kodepost,
0 AS dayalistrik,
0 AS luastanah,
0 AS luasrumah,
a.`rt` AS rt,
a.`rw` AS rw,
NULL AS nohp,
NULL AS keterangan,
IF(a.`nosamb`='-',NULL,a.`nosamb`) AS nosambyangdiberikan,
NULL AS nosambdepan,
NULL AS nosambbelakang,
NULL AS nosambkiri,
NULL AS nosambkanan,
a.`jml_org` AS penghuni,
a.`nmpmlk` AS namapemilik,
NULL AS alamatpemilik,
NULL AS iduser,
n.idnonair AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
IF(a.`no_aktf`<>'-',1,0) AS flagverifikasi,
IF(a.`no_aktf`<>'-',a.`tgl_aktf`,NULL) AS waktuverifikasi,
0 AS flagpelanggankavlingan,
0 AS flaghapus,
a.`tgl_reg` AS waktuupdate,
NULL AS airyangdigunakansebelumnya,
IF(a.`no_aktf`<>'-','Selesai',NULL) AS statuspermohonan
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `maros_awal`.`golonganmaros` g ON g.`kodegolongan`=a.`goltarif`
LEFT JOIN `nonairmaros` n ON n.`nomornonair`=a.`no_reg` AND n.`idjenisnonair`=82
LEFT JOIN `maros_awal`.`rayonmaros` r ON r.`koderayon`=a.`kd_jalan`
LEFT JOIN `maros_awal`.`t_fg_bgn` bg ON bg.`kd_fg_bgn`=a.`fgbgn`
LEFT JOIN `maros_awal`.`kolektifmaros` k ON k.`kodekolektif`=a.`loketkol`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)