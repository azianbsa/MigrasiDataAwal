SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
s.`nomorspkopname` AS `nomorspk`,
s.`tglspko` AS `tanggalspk`,
u.`iduser` AS `iduser`,
s.`disetujui` AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
s.`tglspko` AS `waktuupdate`
FROM `spk_opname_sambung_baru` s
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.`nomorpermohonan`=s.`nomorreg` AND p.idpdam=@idpdam AND p.`idtipepermohonan`=@idtipepermohonan
LEFT JOIN [dataawal].`master_user` u ON u.`nama`=s.`user`
WHERE s.flaghapus=0