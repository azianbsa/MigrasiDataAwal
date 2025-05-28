SET @maxid=(SELECT COALESCE(MAX(idpermohonan),0) AS maxid FROM `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` WHERE idpdam=@idpdam);
SET @idtipepermohonan=(SELECT idtipepermohonan FROM `kotaparepare_dataawal`.`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNG_KEMBALI');

SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idpermohonan,
@idtipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
p.nomor AS nomorpermohonan,
p.tanggal AS waktupermohonan,
r.`idrayon` AS idrayon,
k.`idkelurahan` AS idkelurahan,
g.`idgolongan` AS idgolongan,
NULL AS iddiameter,
pe.`idpelangganair` idpelangganair,
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
NULL AS statuspermohonan,
0 AS flagworkorder,
0 AS flaghapus,
p.`tanggal` AS waktuupdate
FROM `permohonan_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_master_pelanggan_air` pe ON pe.nosamb=p.nosamb
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_rayon` r ON r.koderayon=p.koderayon AND r.`idpdam`=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kelurahan` k ON k.kodekelurahan=p.kodekelurahan AND k.`idpdam`=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_tarif_golongan` g ON g.`kodegolongan`=p.kodegol AND g.status=1 AND g.`idpdam`=@idpdam
,(SELECT @id:=@maxid) AS id
WHERE p.`flaghapus`=0
AND p.`nomor` NOT IN (SELECT `nomorpermohonan` FROM `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` WHERE idpdam=@idpdam AND idtipepermohonan=@idtipepermohonan)
AND DATE_FORMAT(p.`tanggal`,'%Y%m') BETWEEN 202502 AND 202504
